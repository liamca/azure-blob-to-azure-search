using Microsoft.Azure.Search;
using Microsoft.Azure.Search.Models;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using TikaOnDotNet.TextExtraction;

namespace BlobsToAzureSearch
{
    class Program
    {
        private static string BlobService = "[Blob Service Name]";
        private static string BlobKey = "[Blob Key]";
        private static string SourceBlobConectionString = "DefaultEndpointsProtocol=https;AccountName=" + BlobService + ";AccountKey=" + BlobKey + ";";

        private static string SourceBlobContainer = "[Blob Container]";
        private static string SourceBlobFolder = "";        // Optional        

        private static string SearchServiceName = "[Azure Search Service]";
        private static string SearchAdminApiKey = "[Azure Search Service Admin Key]";
        private static string SearchIndexName = "[Azure Search Index Name]";
        private static SearchServiceClient SearchClient = new SearchServiceClient(SearchServiceName, new SearchCredentials(SearchAdminApiKey));

        private static int Parallelism = 16;

        private static DateTime stTime = DateTime.Now;
        private static ConcurrentDictionary<int, List<string>> FilesToProcess = new ConcurrentDictionary<int, List<string>>();

        static void Main(string[] args)
        {
            ServicePointManager.DefaultConnectionLimit = 10000; //(Or More)  

            System.Console.WriteLine(String.Format("Total Min: {0}", DateTime.Now.Subtract(stTime).TotalMinutes));

            Console.WriteLine("{0}", "Deleting index...\n");
            DeleteIndexIfExists(SearchClient, SearchIndexName);

            Console.WriteLine("{0}", "Creating index...\n");
            CreateIndex(SearchClient);

            Console.WriteLine("{0}", "Getting file...\n");
            GetFiles();

            ParallelOptions po = new ParallelOptions();
            po.MaxDegreeOfParallelism = Parallelism;

            int docCounter = 0;

            Parallel.ForEach(FilesToProcess, po, fileList =>
            {
                Console.WriteLine(String.Format("Processing batch #{0}", fileList.Key));
                var textExtractor = new TextExtractor();
                CloudStorageAccount blobStorageAccount = CloudStorageAccount.Parse(SourceBlobConectionString);
                var blobBlobClient = blobStorageAccount.CreateCloudBlobClient();
                var blobContainer = blobBlobClient.GetContainerReference(SourceBlobContainer);
                var containerUrl = blobContainer.Uri.AbsoluteUri;

                SearchIndexClient indexClient = new SearchIndexClient(SearchServiceName, SearchIndexName, new SearchCredentials(SearchAdminApiKey));


                SearchIndexSchema schema = new SearchIndexSchema();
                var indexActionList = new List<IndexAction<SearchIndexSchema>>();


                foreach (var file in fileList.Value)
                {
                    try
                    {

                        Interlocked.Increment(ref docCounter);
                        if (docCounter % 100 == 0)
                            System.Console.WriteLine(String.Format("Completed {0} docs in {1} min...", docCounter, DateTime.Now.Subtract(stTime).TotalMinutes));

                        schema = new SearchIndexSchema();

                        string sasURL = GetBlobSasUri(blobContainer, file);
                        Uri uri = new Uri(sasURL);

                        var result = textExtractor.Extract(uri);
                        var blobMetadata = result.Metadata;

                        schema.content = result.Text;
                        schema.metadata_storage_name = blobContainer.Uri.ToString() + "/" + file;
                        schema.metadata_storage_path = HttpServerUtility.UrlTokenEncode(Encoding.ASCII.GetBytes(schema.metadata_storage_name));

                        schema.metadata_content_type = blobMetadata.ContainsKey("Content-Type") == false ? null : blobMetadata["Content-Type"];
                        schema.metadata_last_modified = blobMetadata.ContainsKey("Last-Modified") == false ? DateTime.Parse("1900-01-01") : DateTimeOffset.Parse(blobMetadata["Last-Modified"]);

                        schema.metadata_word_count = Utilities.WordCount(schema.content);
                        schema.metadata_storage_size = Convert.ToInt32(blobMetadata.ContainsKey("Content-Length") == false ? null : blobMetadata["Content-Length"]);
                        schema.metadata_character_count = schema.content.Length;
                        schema.metadata_author = blobMetadata.ContainsKey("dc:creator") == false ? "" : blobMetadata["dc:creator"];

                        CloudBlockBlob blockBlob = blobContainer.GetBlockBlobReference(file);
                        blockBlob.FetchAttributes();
                        schema.metadata_storage_content_type = blockBlob.Properties.ContentType;
                        schema.metadata_storage_content_md5 = blockBlob.Properties.ContentMD5;
                        schema.metadata_storage_last_modified = DateTimeOffset.Parse(blockBlob.Properties.LastModified.Value.DateTime.ToString());
                        schema.metadata_storage_size = blockBlob.StreamWriteSizeInBytes;

                        var action = IndexAction.Upload(schema);
                        indexActionList.Add(action);

                        //Console.WriteLine("Indexing Counter: " + docCounter);
                        indexClient.Documents.Index(IndexBatch.New(indexActionList));
                    }
                    catch (IndexBatchException ibe)
                    {
                        // Sometimes when your Search service is under load, indexing will fail for some of the documents in
                        // the batch. Depending on your application, you can take compensating actions like delaying and
                        // retrying. For this simple demo, we just log the failed document keys and continue.
                        Console.WriteLine(
                            "Failed to index some of the documents: {0}",
                            String.Join(", ", ibe.IndexingResults.Where(r => !r.Succeeded).Select(r => r.Key)));
                    }
                    catch (Exception ex)
                    {
                        // Sometimes when your Search service is under load, indexing will fail for some of the documents in
                        // the batch. Depending on your application, you can take compensating actions like delaying and
                        // retrying. For this simple demo, we just log the failed document keys and continue.
                        Console.WriteLine(ex.Message);
                    }
                    indexActionList.Clear();

                }

            });

            System.Console.WriteLine(String.Format("Completed {0} docs in {1} min", FilesToProcess.Count, DateTime.Now.Subtract(stTime).TotalMinutes));

        }


        private static void DeleteIndexIfExists(SearchServiceClient serviceClient, string indexName)
        {
            if (serviceClient.Indexes.Exists(indexName))
                serviceClient.Indexes.Delete(indexName);

        }

        private static void CreateIndex(SearchServiceClient serviceClient)
        {
            var definition = new Index()
            {
                Name = SearchIndexName,
                Fields = FieldBuilder.BuildForType<SearchIndexSchema>(),
                Suggesters = new Suggester[]
                {
                    new Suggester()
                    {
                        Name = "sg-" + SearchIndexName,
                        SourceFields = new string[] {"people", "organizations", "locations"}
                    }
                }
            };

            serviceClient.Indexes.Create(definition);

        }

        static string GetBlobSasUri(CloudBlobContainer container, string blobFile)
        {
            //Get a reference to a blob within the container.
            CloudBlockBlob blob = container.GetBlockBlobReference(blobFile);

            //Set the expiry time and permissions for the blob.
            //In this case, the start time is specified as a few minutes in the past, to mitigate clock skew.
            //The shared access signature will be valid immediately.
            SharedAccessBlobPolicy sasConstraints = new SharedAccessBlobPolicy();
            sasConstraints.SharedAccessStartTime = DateTimeOffset.UtcNow.AddMinutes(-5);
            sasConstraints.SharedAccessExpiryTime = DateTimeOffset.UtcNow.AddHours(24);
            sasConstraints.Permissions = SharedAccessBlobPermissions.Read | SharedAccessBlobPermissions.Write;

            //Generate the shared access signature on the blob, setting the constraints directly on the signature.
            string sasBlobToken = blob.GetSharedAccessSignature(sasConstraints);

            //Return the URI string for the container, including the SAS token.
            return blob.Uri + sasBlobToken;
        }

        static void GetFiles()
        {
            Console.WriteLine("Getting list of all files...");
            int fileCounter = 0;
            int keyCounter = 0;
            var fileList = new List<string>();

            try
            {
                CloudStorageAccount blobStorageAccount = CloudStorageAccount.Parse(SourceBlobConectionString);
                var blobBlobClient = blobStorageAccount.CreateCloudBlobClient();
                var blobContainer = blobBlobClient.GetContainerReference(SourceBlobContainer);
                foreach (var file in blobContainer.ListBlobs(SourceBlobFolder, true))
                {
                    fileCounter++;
                    fileList.Add(((CloudBlob)file).Name);
                    if (fileCounter % 100 == 0)
                    {
                        FilesToProcess.TryAdd(keyCounter, fileList);
                        fileList = new List<string>();
                        keyCounter++;
                    }
                    if (fileCounter % 100000 == 0)
                        Console.WriteLine(String.Format("Retrieved {0} files...", fileCounter));

                }
                FilesToProcess.TryAdd(keyCounter, fileList);
                Console.WriteLine(String.Format("Applied {0} files to SQLIte db...", fileCounter));
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            Console.WriteLine(String.Format("Retrieved File Count: {0}", fileCounter));
        }

    }
}

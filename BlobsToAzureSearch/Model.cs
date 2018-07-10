using Microsoft.Azure.Search;
using Microsoft.Azure.Search.Models;
using System;

namespace BlobsToAzureSearch
{
    [SerializePropertyNamesAsCamelCase]
    public class SearchIndexSchema
    {
        [IsSearchable, Analyzer("en.microsoft")]
        public string content { get; set; }

        [IsSearchable, IsFacetable, IsFilterable]
        public string metadata_storage_content_type { get; set; }

        [IsFilterable, IsFacetable, IsSortable]
        public Int64 metadata_storage_size { get; set; }

        [IsFilterable, IsFacetable, IsSortable]
        public DateTimeOffset metadata_storage_last_modified { get; set; }

        public string metadata_storage_content_md5 { get; set; }

        [IsSearchable, Analyzer("en.microsoft")]
        public string metadata_storage_name { get; set; }

        [System.ComponentModel.DataAnnotations.Key]
        public string metadata_storage_path { get; set; }

        [IsFilterable, IsFacetable, IsSortable]
        public string metadata_content_type { get; set; }

        [IsSearchable, IsFilterable, IsFacetable, Analyzer("en.microsoft")]
        public string metadata_author { get; set; }

        [IsFilterable, IsFacetable, IsSortable]
        public int metadata_character_count { get; set; }

        [IsFilterable, IsFacetable, IsSortable]
        public DateTimeOffset metadata_creation_date { get; set; }

        [IsFilterable, IsFacetable, IsSortable]
        public DateTimeOffset metadata_last_modified { get; set; }

        [IsFilterable, IsFacetable, IsSortable]
        public int metadata_page_count { get; set; }

        [IsFilterable, IsFacetable, IsSortable]
        public int metadata_word_count { get; set; }

        [IsSearchable, IsFilterable, IsFacetable, Analyzer("en.microsoft")]
        public string[] people { get; set; }

        [IsSearchable, IsFilterable, IsFacetable, Analyzer("en.microsoft")]
        public string[] organizations { get; set; }

        [IsSearchable, IsFilterable, IsFacetable, Analyzer("en.microsoft")]
        public string[] locations { get; set; }

        [IsSearchable, IsFilterable, IsFacetable, Analyzer("en.microsoft")]
        public string[] keyphrases { get; set; }

        [IsFilterable, IsFacetable]
        public string language { get; set; }

        [IsSearchable, Analyzer("en.microsoft")]
        public string merged_content { get; set; }

        [IsSearchable, Analyzer("en.microsoft")]
        public string text { get; set; }

        public string layoutText { get; set; }

    }
}

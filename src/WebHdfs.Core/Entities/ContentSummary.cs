using Newtonsoft.Json.Linq;

namespace WebHdfs.Core.Entities
{
    /// <summary>
    /// Content summary.
    /// </summary>
    /// <inheritdoc cref="IJObject" />
    public class ContentSummary : IJObject
    {
        /// <summary>
        /// The number of directories.
        /// </summary>
        public int DirectoryCount
        { get; set; }

        /// <summary>
        /// The number of files.
        /// </summary>
        public int FileCount
        { get; set; }

        /// <summary>
        /// The number of bytes used by the content.
        /// </summary>
        public long Length
        { get; set; }

        /// <summary>
        /// The namespace quota of this directory.
        /// </summary>
        public int Quota
        { get; set; }

        /// <summary>
        /// The disk space consumed by the content.
        /// </summary>
        public long SpaceConsumed
        { get; set; }

        /// <summary>
        /// The disk space quota.
        /// </summary>
        public long SpaceQuota
        { get; set; }

        /// <inheritdoc />
        public void Parse(JObject value)
        {
            var tmp = value.Value<JObject>("ContentSummary") ?? value;

            DirectoryCount = tmp.Value<int>("directoryCount");
            FileCount = tmp.Value<int>("fileCount");
            Length = tmp.Value<long>("length");
            Quota = tmp.Value<int>("quota");
            SpaceConsumed = tmp.Value<long>("spaceConsumed");
            SpaceQuota = tmp.Value<long>("spaceQuota");
        }
    }
}
using System.Collections.Generic;
using System;
using System.Linq;
using Newtonsoft.Json.Linq;

namespace WebHdfs.Core.Entities
{
    /// <summary>
    /// Entity for Directory listing.
    /// </summary>
    /// <inheritdoc cref="IJObject" />
    public class DirectoryItems : IJObject
    {
        IEnumerable<FileStatus> directoryEntries;

        /// <inheritdoc />
        public void Parse(JObject rootEntry)
        {
            directoryEntries = rootEntry.Value<JObject>("FileStatuses").Value<JArray>("FileStatus").Select(fs =>
            {
                var d = new FileStatus();
                d.Parse(fs.Value<JObject>());
                return d;
            });
        }

        /// <summary>
        /// List of subdirectories 
        /// </summary>
        public IEnumerable<FileStatus> Directories
        { get { return directoryEntries.Where(fs => fs.Type == "DIRECTORY"); } }

        /// <summary>
        /// List of files
        /// </summary>
        public IEnumerable<FileStatus> Files
        { get { return directoryEntries.Where(fs => fs.Type == "FILE"); } }
    }
}

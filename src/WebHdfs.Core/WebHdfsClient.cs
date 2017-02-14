using System;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using WebHdfs.Core.Entities;
using System.Text;
using System.Text.RegularExpressions;
using Flurl;
using System.Collections.Generic;

namespace WebHdfs.Core
{
    /// <summary>
    /// Minimalistic WebHdfs client
    /// </summary>
    public class WebHdfsClient : IDisposable
    {
        private HttpClient httpClient { get; set; }
        /// <summary>
        /// Base url of WebHdfs service.
        /// </summary>
        public string BaseUrl
        {
            get
            {
                return httpClient.BaseAddress.ToString();
            }
        }

        /// <summary>
        /// Home directory.
        /// </summary>
        private string homeDirectory { get; set; }

        /// <summary>
        /// Username to be used with securify off (when only user.name required);
        /// </summary>
        public string User { get; private set; }


        public string Prefix { get; private set; }

        public const string DefaultPrefix = "/webhdfs/v1/";
        string trashFolder
        {
            get
            {
                return $"/user/{User}/.Trash/Current";
            }
        }

        const string _defaultPermissions = "755";
        private const int _defaultTimeoutSeconds = 300;

        /// <summary>
        /// Underlying <see cref="HttpMessageHandler"/> that will process web requests (for testing purpose mostly).
        /// </summary>
        public HttpMessageHandler InnerHandler
        { get; set; }

        //private string GetAbsolutePath(string hdfsPath)
        //{
        //    if (string.IsNullOrEmpty(hdfsPath))
        //    {
        //        return "/";
        //    }
        //    else if (hdfsPath[0] == '/')
        //    {
        //        return hdfsPath;
        //    }
        //    else if (hdfsPath.Contains(":"))
        //    {
        //        Uri uri = new Uri(hdfsPath);
        //        return uri.AbsolutePath;
        //    }
        //    else
        //    {
        //        return HomeDirectory.AppendPathSegment(hdfsPath);
        //    }
        //}

        //private string GetFullyQualifiedPath(string remotePath)
        //{
        //    if (remotePath.Contains(":"))
        //    {
        //        return remotePath;
        //    }

        //    remotePath = GetAbsolutePath(remotePath);
        //    return "hdfs://" + BaseUrl + remotePath;
        //}

        /// <summary>
        /// Public constructor.
        /// </summary>
        /// <param name="baseUrl">Base url of WebHdfs service.</param>
        /// <param name="user">Username to be used on each call.</param>
        public WebHdfsClient(string baseUrl, string user = null, string prefix = DefaultPrefix, int timeoutSeconds = _defaultTimeoutSeconds)
            : this(new HttpClientHandler(), baseUrl, user, prefix, timeoutSeconds)
        {
        }

        /// <summary>
        /// Public constructor.
        /// </summary>
        /// <param name="handler">Underlying <see cref="HttpMessageHandler"/> to be used (for testing mostly).</param>
        /// <param name="baseUrl">Base url of WebHdfs service.</param>
        /// <param name="user">Username to be used on each call.</param>
        public WebHdfsClient(HttpMessageHandler handler, string baseUrl, string user = null, string prefix = DefaultPrefix, int timeoutSeconds = _defaultTimeoutSeconds)
        {
            InnerHandler = handler;
            httpClient = new HttpClient(handler) { BaseAddress = new Uri(baseUrl.AppendPathSegment(prefix)), Timeout = TimeSpan.FromSeconds(timeoutSeconds) };
            User = user;
            Prefix = prefix;
            //GetHomeDirectory().Wait();
        }

        #region "read"

        /// <summary>
        /// List the statuses of the files/directories in the given path if the path is a directory. 
        /// </summary>
        /// <param name="remotePath">The string representation a Path.</param>
        /// <returns></returns>
        public async Task<DirectoryItems> GetDirectoryItemsAsync(string remotePath)
        {
            try
            {
                return await callWebHDFS<DirectoryItems>(remotePath, "LISTSTATUS", HttpMethod.Get);
            }
            catch (WebHdfsException ex) when (ex.Response?.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                return null;
            }
        }

        /// <summary>
        /// Return a file status object that represents the path.
        /// </summary>
        /// <param name="remotePath">The string representation a Path.</param>
        /// <returns></returns>
        public async Task<FileStatus> GetStatusAsync(string remotePath)
        {
            try
            {
                return await callWebHDFS<FileStatus>(remotePath, "GETFILESTATUS", HttpMethod.Get);
            }
            catch (WebHdfsException ex) when (ex.Response?.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                return null;
            }
        }

        public async Task<bool> ExistsAsync(string remotePath)
        {
            try
            {
                var dummy = await callWebHDFS<FileStatus>(remotePath, "GETFILESTATUS", HttpMethod.Get);
                return true;
            }
            catch (WebHdfsException ex) when (ex.Response?.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                return false;
            }
        }

        /// <summary>
        /// Return the current user's home directory in this filesystem. 
        /// The default implementation returns "/user/$USER/". 
        /// </summary>
        /// <returns></returns>
        public async Task GetHomeDirectoryAsync()
        {
            if (string.IsNullOrEmpty(homeDirectory))
            {
                string uri = prepareUrl("/", "GETHOMEDIRECTORY");
                var response = await getResponseMessageAsync(uri);
                if (response.IsSuccessStatusCode)
                {
                    JObject path = JObject.Parse(await response.Content.ReadAsStringAsync());
                    homeDirectory = path.Value<string>("Path");
                }
            }
        }

        /// <summary>
        /// Return the ContentSummary of a given Path
        /// </summary>
        /// <param name="remotePath">The string representation a Path.</param>
        /// <returns></returns>
        public async Task<ContentSummary> GetContentSummaryAsync(string remotePath)
        {
            try
            {
                return await callWebHDFS<ContentSummary>(remotePath, "GETCONTENTSUMMARY", HttpMethod.Get);
            }
            catch (WebHdfsException ex) when (ex.Response?.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                return null;
            }
        }

        /// <summary>
        /// Get the checksum of a file
        /// </summary>
        /// <param name="remotePath">The file checksum. The default return value is null, which 
        /// indicates that no checksum algorithm is implemented in the corresponding FileSystem. </param>
        /// <returns></returns>
        public async Task<FileChecksum> GetFileChecksumAsync(string remotePath)
        {
            try
            {
                return await callWebHDFS<FileChecksum>(remotePath, "GETFILECHECKSUM", HttpMethod.Get);
            }
            catch (WebHdfsException ex) when (ex.Response?.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                return null;
            }
        }

        /// <summary>
        /// Opens an FSDataInputStream at the indicated Path
        /// </summary>
        /// <param name="remotePath">The string representation a Path.</param>
        /// <returns>Async <see cref="Task{Stream}"/> with file content.</returns>
        public async Task<Stream> OpenFileReadAsync(string remotePath)
        {
            return await OpenFileReadAsync(remotePath, -1, -1, CancellationToken.None);
        }

        /// <summary>
        /// Opens an FSDataInputStream at the indicated Path
        /// </summary>
        /// <param name="remotePath">The string representation a Path.</param>
        /// <param name="token"><see cref="CancellationToken"/> to cancel call if needed.</param>
        /// <returns>Async <see cref="Task{Stream}"/> with file content.</returns>
        public async Task<Stream> OpenFileReadAsync(string remotePath, CancellationToken token)
        {
            return await OpenFileReadAsync(remotePath, -1, -1, token);
        }

        /// <summary>
        /// Opens an FSDataInputStream at the indicated Path.  The offset and length will allow 
        /// you to get a subset of the file.  
        /// </summary>
        /// <param name="remotePath">The string representation a Path.</param>
        /// <param name="offset">The starting byte position. This includes any header bytes</param>
        /// <param name="length">The number of bytes to be processed.</param>
        /// <param name="token"><see cref="CancellationToken"/> to cancel call if needed.</param>
        /// <returns>Async <see cref="Task{Stream}"/> with file content.</returns>
        public async Task<Stream> OpenFileReadAsync(string remotePath, int offset = 0, int length = -1, CancellationToken token = default(CancellationToken))
        {
            string uri = prepareUrl(remotePath, "OPEN");
            if (offset > 0)
                uri += uri.SetQueryParam("offset", offset.ToString());
            if (length > 0)
                uri += uri.SetQueryParam("length", length.ToString());
            var request = new HttpRequestMessage(HttpMethod.Get, uri);
            var response = await httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, token);
            return await response.Content.ReadAsStreamAsync();
        }

        /// <summary>
        /// Download a file from HDFS
        /// </summary>
        /// <param name="remotePath"></param>
        /// <param name="destinationPath"></param>
        /// <param name="overwrite"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        public async Task<bool> DownloadFileAsync(string remotePath, string destinationPath, bool overwrite = false, CancellationToken token = default(CancellationToken))
        {
            if (string.IsNullOrWhiteSpace(remotePath))
            {
                throw new ArgumentException("Path must not be empty", nameof(remotePath));
            }

            if (string.IsNullOrWhiteSpace(destinationPath))
            {
                throw new ArgumentException("Path must not be empty", nameof(destinationPath));
            }

            if (!overwrite && File.Exists(destinationPath))
            {
                throw new IOException("Destination file already exists.");
            }

            using (var inputStream = await OpenFileReadAsync(remotePath))
            {
                using (var outputStream = File.OpenWrite(destinationPath))
                {
                    await inputStream.CopyToAsync(outputStream);
                }
            }

            return true;
        }

        /// <summary>
        /// Download a directory from HDFS
        /// </summary>
        /// <param name="remoteDirectory"></param>
        /// <param name="localDirectory"></param>
        /// <param name="overwrite"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        public async Task<bool> DownloadDirectoryAsync(string remoteDirectory, string localDirectory, bool overwrite = false, CancellationToken token = default(CancellationToken))
        {
            if (string.IsNullOrWhiteSpace(remoteDirectory))
            {
                throw new ArgumentException("Path must not be empty", nameof(remoteDirectory));
            }

            if (string.IsNullOrWhiteSpace(localDirectory))
            {
                throw new ArgumentException("Path must not be empty", nameof(localDirectory));
            }

            Directory.CreateDirectory(localDirectory);

            var list = await GetDirectoryItemsAsync(remoteDirectory);

            if (list == null)
            {
                throw new DirectoryNotFoundException($"Directory \"{remoteDirectory}\" not exists");
            }

            foreach (var file in list.Files)
            {
                await DownloadFileAsync(remoteDirectory.AppendPathSegment(file.PathSuffix), Path.Combine(localDirectory, file.PathSuffix), overwrite);
            }

            foreach (var childDir in list.Directories)
            {
                var childDestDir = Path.Combine(localDirectory, childDir.PathSuffix);

                await DownloadDirectoryAsync(remoteDirectory.AppendPathSegment(childDir.PathSuffix), childDestDir, overwrite);
            }

            return true;
        }

        #endregion

        #region divers
        /// <summary>
        /// Make the given file and all non-existent parents into directories. 
        /// Has the semantics of Unix 'mkdir -p'. Existence of the directory hierarchy is not an error. 
        /// </summary>
        /// <param name="remotePath">The string representation a Path.</param>
        /// <returns>Async <see cref="Task{Boolean}"/> with result of operation.</returns>
        public async Task<bool> CreateDirectoryAsync(string remotePath, string permissions = _defaultPermissions)
        {
            object additionalQueryParameters = null;
            throwIfPermissionsNotValide(permissions);
            additionalQueryParameters = new { permission = permissions };

            var result = await callWebHDFS<BooleanResult>(remotePath, "MKDIRS", HttpMethod.Put, additionalQueryParameters: additionalQueryParameters);
            return result.Value;
        }

        /// <summary>
        /// Renames Path src to Path dst.
        /// </summary>
        /// <param name="remotePath">The string representation a Path.</param>
        /// <param name="newRemotePath"></param>
        /// <returns>Async <see cref="Task{Boolean}"/> with result of operation.</returns>
        public async Task<bool> RenameAsync(string remotePath, string newRemotePath)
        {
            var result = await callWebHDFS<BooleanResult>(remotePath, "RENAME", HttpMethod.Put, additionalQueryParameters: new { destination = newRemotePath });
            return result.Value;
        }

        /// <summary>
        /// Delete a file.  Note, this will not recursively delete and will
        /// not delete if directory is not empty
        /// </summary>
        /// <param name="remotePath">the path to delete</param>
        /// <returns>Async <see cref="Task{Boolean}"/> with result of operation.</returns>
        public async Task<bool> DeleteAsync(string remotePath)
        {
            return await DeleteAsync(remotePath, false, false);
        }

        /// <summary>
        /// Delete a file
        /// </summary>
        /// <param name="remotePath">the path to delete</param>
        /// <param name="recursive">if path is a directory and set to true, the directory is deleted else throws an exception.
        /// In case of a file the recursive can be set to either true or false. </param>
        /// <returns>Async <see cref="Task{Boolean}"/> with result of operation.</returns>
        public async Task<bool> DeleteAsync(string remotePath, bool recursive = false, bool skipTrash = false)
        {
            if (skipTrash)
            {
                return (await callWebHDFS<BooleanResult>(remotePath, "DELETE", HttpMethod.Delete, additionalQueryParameters: new { recursive = recursive.ToString().ToLower() })).Value;
            }
            else
            {
                await CreateDirectoryAsync(trashFolder);
                var newName = trashFolder.AppendPathSegment(Path.GetFileName(remotePath));
                var baseNewName = newName;
                int c = 2;
                while (await ExistsAsync(newName))
                {
                    newName = trashFolder.AppendPathSegment(Path.GetFileNameWithoutExtension(remotePath) + $"({c})" + Path.GetExtension(remotePath));
                    c++;
                }
                return await RenameAsync(remotePath, newName);
            }
        }
        #endregion

        #region Set File Attributes

        /// <summary>
        /// Set permission of a path.
        /// </summary>
        /// <param name="remotePath">The string representation a Path.</param>
        /// <param name="permissions"></param>
        /// <returns>Async <see cref="Task{Boolean}"/> with result of operation.</returns>
        public async Task<bool> SetPermissionsAsync(string remotePath, string permissions)
        {
            throwIfPermissionsNotValide(permissions);

            var result = await callWebHDFS<BooleanResult>(remotePath, "SETPERMISSION", HttpMethod.Put, additionalQueryParameters: new { permission = permissions });
            return result.Value;
        }

        /// <summary>
        /// Sets the owner for the file 
        /// </summary>
        /// <param name="remotePath">The string representation a Path.</param>
        /// <param name="owner">If it is null, the original username remains unchanged</param>
        /// <returns>Async <see cref="Task{Boolean}"/> with result of operation.</returns>
        public async Task<bool> SetOwnerAsync(string remotePath, string owner)
        {
            var result = await callWebHDFS<BooleanResult>(remotePath, "SETOWNER", HttpMethod.Put, additionalQueryParameters: new { owner = owner });
            return result.Value;
        }

        /// <summary>
        /// Sets the group for the file 
        /// </summary>
        /// <param name="remotePath">The string representation a Path.</param>
        /// <param name="group">If it is null, the original groupname remains unchanged</param>
        /// <returns>Async <see cref="Task{Boolean}"/> with result of operation.</returns>
        public async Task<bool> SetGroupAsync(string remotePath, string group)
        {
            var result = await callWebHDFS<BooleanResult>(remotePath, "SETOWNER", HttpMethod.Put, additionalQueryParameters: new { group = group });
            return result.Value;
        }

        /// <summary>
        /// Set replication for an existing file.
        /// </summary>
        /// <param name="remotePath">The string representation a Path.</param>
        /// <param name="replicationFactor"></param>
        /// <returns>Async <see cref="Task{Boolean}"/> with result of operation.</returns>
        public async Task<bool> SetReplicationFactorAsync(string remotePath, int replicationFactor)
        {
            var result = await callWebHDFS<BooleanResult>(remotePath, "SETREPLICATION", HttpMethod.Put, additionalQueryParameters: new { replication = replicationFactor });
            return result.Value;
        }

        /// <summary>
        /// Set access time of a file
        /// </summary>
        /// <param name="remotePath">The string representation a Path.</param>
        /// <param name="accessTime">Set the access time of this file. The number of milliseconds since Jan 1, 1970. 
        /// A value of -1 means that this call should not set access time</param>
        /// <returns>Async <see cref="Task{Boolean}"/> with result of operation.</returns>
        public async Task<bool> SetAccessTimeAsync(string remotePath, string accessTime)
        {
            var result = await callWebHDFS<BooleanResult>(remotePath, "SETTIMES", HttpMethod.Put, additionalQueryParameters: new { accesstime = accessTime });
            return result.Value;
        }

        /// <summary>
        /// Set modification time of a file
        /// </summary>
        /// <param name="remotePath">The string representation a Path.</param>
        /// <param name="modificationTime">Set the modification time of this file. The number of milliseconds since Jan 1, 1970.
        /// A value of -1 means that this call should not set modification time</param>
        /// <returns>Async <see cref="Task{Boolean}"/> with result of operation.</returns>
        public async Task<bool> SetModificationTimeAsync(string remotePath, string modificationTime)
        {
            var result = await callWebHDFS<BooleanResult>(remotePath, "SETTIMES", HttpMethod.Put, additionalQueryParameters: new { modificationtime = modificationTime });
            return result.Value;
        }
        #endregion

        #region CreateFile
        /// <summary>
        /// Opens an FSDataOutputStream at the indicated Path.
        /// </summary>
        /// <param name="localFile"></param>
        /// <param name="remotePath"></param>
        /// <param name="overwrite"></param>
        /// <param name="token"></param>
        /// <returns>Async <see cref="Task{Boolean}"/> with result of operation.</returns>
        public async Task<bool> UploadFileAsync(string localFile, string remotePath, bool overwrite = false, string permissions = _defaultPermissions, CancellationToken token = default(CancellationToken))
        {
            using (var stream = File.OpenRead(localFile))
            {
                var sc = new StreamContent(stream);
                return await createFile(sc, remotePath, overwrite, permissions, token);
            }
        }

        public async Task<bool> UploadDirectoryAsync(string localDirectory, string remoteDirectory, bool overwrite = false, CancellationToken token = default(CancellationToken))
        {
            if (string.IsNullOrWhiteSpace(localDirectory))
            {
                throw new ArgumentException("Path must not be empty", nameof(localDirectory));
            }

            if (string.IsNullOrWhiteSpace(remoteDirectory))
            {
                throw new ArgumentException("Path must not be empty", nameof(remoteDirectory));
            }

            if (!Directory.Exists(localDirectory))
            {
                throw new DirectoryNotFoundException($"Directory \"{localDirectory}\" not exists");
            }

            foreach (var file in Directory.GetFiles(localDirectory))
            {
                await UploadFileAsync(file, remoteDirectory.AppendPathSegment(Path.GetFileName(file)));
            }

            foreach (var dir in Directory.GetDirectories(localDirectory))
            {
                await UploadDirectoryAsync(dir, remoteDirectory.AppendPathSegment(Path.GetDirectoryName(dir)));
            }

            return true;
        }


        /// <summary>
        /// Opens an FSDataOutputStream at the indicated Path.
        /// </summary>
        /// <param name="content"></param>
        /// <param name="remotePath"></param>
        /// <param name="overwrite"></param>
        /// <param name="permissions"></param>
        /// <param name="token"></param>
        /// <returns>Async <see cref="Task{Boolean}"/> with result of operation.</returns>
        public async Task<bool> CreateFileAsync(Stream content, string remotePath, bool overwrite = false, string permissions = _defaultPermissions, CancellationToken token = default(CancellationToken))
        {
            var sc = new StreamContent(content);
            return await createFile(sc, remotePath, overwrite, permissions, token);
        }

        /// <summary>
        /// Opens an FSDataOutputStream at the indicated Path.
        /// </summary>
        /// <param name="byteArray"></param>
        /// <param name="remotePath"></param>
        /// <param name="overwrite"></param>
        /// <param name="permissions"></param>
        /// <param name="token"></param>
        /// <returns>Async <see cref="Task{Boolean}"/> with result of operation.</returns>
        public async Task<bool> CreateFileAsync(byte[] byteArray, string remotePath, bool overwrite = false, string permissions = _defaultPermissions, CancellationToken token = default(CancellationToken))
        {
            var sc = new ByteArrayContent(byteArray);
            return await createFile(sc, remotePath, overwrite, permissions, token);
        }

        /// <summary>
        /// Opens an FSDataOutputStream at the indicated Path.
        /// </summary>
        /// <param name="text"></param>
        /// <param name="encoding">If null Utf8 will be used</param>
        /// <param name="remotePath"></param>
        /// <param name="overwrite"></param>
        /// <param name="permissions"></param>
        /// <param name="token"></param>
        /// <returns>Async <see cref="Task{Boolean}"/> with result of operation.</returns>
        public async Task<bool> CreateFileAsync(string text, Encoding encoding, string remotePath, bool overwrite = false, string permissions = _defaultPermissions, CancellationToken token = default(CancellationToken))
        {
            var sc = new ByteArrayContent((encoding ?? Encoding.UTF8).GetBytes(text));
            return await createFile(sc, remotePath, overwrite, permissions, token);
        }

        private async Task<bool> createFile(HttpContent content, string remotePath, bool overwrite, string permissions = _defaultPermissions, CancellationToken token = default(CancellationToken))
        {
            var addingUrl = prepareUrl(remotePath, "CREATE", new { overwrite = overwrite.ToString().ToLower() });
            var location = await getRedirectLocation(addingUrl, HttpMethod.Put, token);

            var response = await getResponseMessageAsync(location.ToString(), content, new WebHdfsRequestOptions { Method = HttpMethod.Put, Token = token });
            if (response.StatusCode == System.Net.HttpStatusCode.Created)
            {
                return true;
            }
            else
            {
                throw new WebHdfsException(response, "File was not created error. See details for plus information.");
            }
        }
        #endregion

        #region AppendFile
        /// <summary>
        /// Opens an FSDataOutputStream at the indicated Path.
        /// </summary>
        /// <param name="localFile"></param>
        /// <param name="remotePath"></param>
        /// <param name="token"></param>
        /// <returns>Async <see cref="Task{Boolean}"/> with result of operation.</returns>
        public async Task<bool> AppendFileAsync(string localFile, string remotePath, CancellationToken token = default(CancellationToken))
        {
            using (var stream = File.OpenRead(localFile))
            {
                var sc = new StreamContent(stream);
                return await appendFile(sc, remotePath, token: token);
            }
        }

        /// <summary>
        /// Opens an FSDataOutputStream at the indicated Path.
        /// </summary>
        /// <param name="content"></param>
        /// <param name="remotePath"></param>
        /// <param name="token"></param>
        /// <returns>Async <see cref="Task{Boolean}"/> with result of operation.</returns>
        public async Task<bool> AppendFileAsync(Stream content, string remotePath, CancellationToken token = default(CancellationToken))
        {
            var sc = new StreamContent(content);
            return await appendFile(sc, remotePath, token: token);
        }

        /// <summary>
        /// Opens an FSDataOutputStream at the indicated Path.
        /// </summary>
        /// <param name="byteArray"></param>
        /// <param name="remotePath"></param>
        /// <param name="token"></param>
        /// <returns>Async <see cref="Task{Boolean}"/> with result of operation.</returns>
        public async Task<bool> AppendFileAsync(byte[] byteArray, string remotePath, CancellationToken token = default(CancellationToken))
        {
            var content = new ByteArrayContent(byteArray);
            return await appendFile(content, remotePath, token: token);
        }

        /// <summary>
        /// Opens an FSDataOutputStream at the indicated Path.
        /// </summary>
        /// <param name="text"></param>
        /// <param name="encoding">If null Utf8 will be used</param>
        /// <param name="remotePath"></param>
        /// <param name="token"></param>
        /// <returns>Async <see cref="Task{Boolean}"/> with result of operation.</returns>
        public async Task<bool> AppendFileAsync(string text, Encoding encoding, string remotePath, CancellationToken token = default(CancellationToken))
        {
            var content = new ByteArrayContent((encoding ?? Encoding.UTF8).GetBytes(text));
            return await appendFile(content, remotePath, token: token);
        }

        private async Task<bool> appendFile(HttpContent content, string remotePath, CancellationToken token = default(CancellationToken))
        {
            var addingUrl = prepareUrl(remotePath, "APPEND");
            var location = await getRedirectLocation(addingUrl, HttpMethod.Post, token);

            var response = await getResponseMessageAsync(location.ToString(), content, new WebHdfsRequestOptions { Method = HttpMethod.Post, Token = token });
            if (response.StatusCode == System.Net.HttpStatusCode.OK)
            {
                return true;
            }
            else
            {
                throw new WebHdfsException(response, "File was not appended error. See details for plus information.");
            }
        }
        #endregion

        #region comunication
        private Task<T> callWebHDFS<T>(string remotePath, string operation, HttpMethod method, HttpContent content = null, object additionalQueryParameters = null) where T : IJObject, new()
        {
            return callWebHDFS<T>(remotePath, operation, content, new WebHdfsRequestOptions() { Method = method, AdditionalQueryParameters = additionalQueryParameters });
        }

        private async Task<T> callWebHDFS<T>(string remotePath, string operation, HttpContent content = null, WebHdfsRequestOptions options = null) where T : IJObject, new()
        {
            string uri = prepareUrl(remotePath, operation, options.AdditionalQueryParameters);

            var response = await getResponseMessageAsync(uri, content, options ?? new WebHdfsRequestOptions());

            if (response.IsSuccessStatusCode)
            {
                try
                {
                    var jobj = JObject.Parse(await response.Content.ReadAsStringAsync());

                    if (jobj == null)
                        return default(T);

                    var result = new T() as IJObject;
                    result.Parse(jobj);
                    return (T)result;
                }
                catch (Exception ex)
                {
                    throw new WebHdfsException(response, "Result Json parsing error", ex);
                }
            }

            return default(T);
        }

        private string prepareUrl(string remotePath, string operation, object additionalQueryParameters = null)
        {
            if (string.IsNullOrWhiteSpace(remotePath)) remotePath = "";

            remotePath = remotePath.TrimStart('/');

            remotePath = remotePath.SetQueryParam("op", operation);

            if (!string.IsNullOrEmpty(User))
                remotePath = remotePath.SetQueryParam("user.name", User);

            if (additionalQueryParameters != null)
            {
                remotePath = remotePath.SetQueryParams(additionalQueryParameters);
            }

            return remotePath;
        }

        /// <summary>
        /// Send http request and return <see cref="HttpResponseMessage"/> received.
        /// </summary>
        /// <param name="url">Url to be requested.</param>
        /// <param name="data">Data to be sent.</param>
        /// <param name="options">Request options.</param>
        /// <returns>Asynchronous task.</returns>
        private async Task<HttpResponseMessage> getResponseMessageAsync(string url, HttpContent data = null, WebHdfsRequestOptions options = null)
        {
            if (options == null)
                options = new WebHdfsRequestOptions();

            var request = new HttpRequestMessage(options.Method, url);

            if (options.Method != HttpMethod.Get && data != null)
            {
                request.Content = data;
            }

            var response = await httpClient.SendAsync(request, options.Completion, options.Token).ConfigureAwait(false);

            if (!response.IsSuccessStatusCode)
            {
                throw new WebHdfsException(response);
            }

            return response;
        }

        private async Task<Uri> getRedirectLocation(string url, HttpMethod method, CancellationToken token = default(CancellationToken))
        {
            using (var client = new HttpClient(new HttpClientHandler() { AllowAutoRedirect = false }, true))
            {
                client.Timeout = httpClient.Timeout;
                client.BaseAddress = httpClient.BaseAddress;

                //client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                //client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/octet-stream"));

                var request = new HttpRequestMessage(method, url);

                var response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, token).ConfigureAwait(false);

                if ((int)response.StatusCode >= 400)
                {
                    throw new WebHdfsException(response);
                }

                return response.Headers.Location;
            }
        }
        #endregion

        #region helpers

        private bool throwIfPermissionsNotValide(string permissions)
        {
            if (Regex.IsMatch(permissions, "^[01][0-7]{0,3}$"))
            {
                throw new ArgumentException(nameof(permissions), "The value must be any radix-8 integer between 0 and 1777.");
            }

            return true;
        }

        #endregion

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    InnerHandler?.Dispose();
                    httpClient?.Dispose();
                    // TODO: dispose managed state (managed objects).
                }

                // TODO: free unmanaged resources (unmanaged objects) and override a finalizer below.
                // TODO: set large fields to null.

                disposedValue = true;
            }
        }

        // TODO: override a finalizer only if Dispose(bool disposing) above has code to free unmanaged resources.
        // ~WebHdfsClient() {
        //   // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
        //   Dispose(false);
        // }

        // This code added to correctly implement the disposable pattern.
        void IDisposable.Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
            // TODO: uncomment the following line if the finalizer is overridden above.
            // GC.SuppressFinalize(this);
        }
        #endregion
    }
}
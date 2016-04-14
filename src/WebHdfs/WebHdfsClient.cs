﻿using System;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using WebHdfs.Entities;
using System.Text;
using System.Text.RegularExpressions;
using Flurl;
using System.Collections.Generic;

namespace WebHdfs
{
    /// <summary>
    /// Minimalistic WebHdfs client
    /// </summary>
    public class WebHdfsClient
    {
        /// <summary>
        /// Base url of WebHdfs service.
        /// </summary>
        public string BaseUrl { get; private set; }

        /// <summary>
        /// Home directory.
        /// </summary>
        private string homeDirectory { get; set; }

        /// <summary>
        /// Username to be used with securify off (when only user.name required);
        /// </summary>
        public string User { get; private set; }


        public string Prefix { get; private set; }

        const string _defaultPrefix = "webhdfs/v1";

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

        //private string GetFullyQualifiedPath(string path)
        //{
        //    if (path.Contains(":"))
        //    {
        //        return path;
        //    }

        //    path = GetAbsolutePath(path);
        //    return "hdfs://" + BaseUrl + path;
        //}

        /// <summary>
        /// Public constructor.
        /// </summary>
        /// <param name="baseUrl">Base url of WebHdfs service.</param>
        /// <param name="user">Username to be used on each call.</param>
        public WebHdfsClient(string baseUrl, string user = null, string prefix = _defaultPrefix)
            : this(new HttpClientHandler(), baseUrl, user, prefix)
        {
        }

        /// <summary>
        /// Public constructor.
        /// </summary>
        /// <param name="handler">Underlying <see cref="HttpMessageHandler"/> to be used (for testing mostly).</param>
        /// <param name="baseUrl">Base url of WebHdfs service.</param>
        /// <param name="user">Username to be used on each call.</param>
        public WebHdfsClient(HttpMessageHandler handler, string baseUrl, string user = null, string prefix = _defaultPrefix)
        {
            InnerHandler = handler;
            BaseUrl = baseUrl;
            User = user;
            Prefix = prefix;
            //GetHomeDirectory().Wait();
        }

        #region "read"

        /// <summary>
        /// List the statuses of the files/directories in the given path if the path is a directory. 
        /// </summary>
        /// <param name="path">The string representation a Path.</param>
        /// <returns></returns>
        public Task<DirectoryListing> GetDirectoryStatus(string path)
        {
            return callWebHDFS<DirectoryListing>(path, "LISTSTATUS", HttpMethod.Get);
        }

        /// <summary>
        /// Return a file status object that represents the path.
        /// </summary>
        /// <param name="path">The string representation a Path.</param>
        /// <returns></returns>
        public Task<DirectoryEntry> GetFileStatus(string path)
        {
            return callWebHDFS<DirectoryEntry>(path, "GETFILESTATUS", HttpMethod.Get);
        }

        /// <summary>
        /// Return the current user's home directory in this filesystem. 
        /// The default implementation returns "/user/$USER/". 
        /// </summary>
        /// <returns></returns>
        public async Task GetHomeDirectory()
        {
            if (string.IsNullOrEmpty(homeDirectory))
            {
                string uri = GetUriForOperation("/").SetQueryParam("op", "GETHOMEDIRECTORY");
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
        /// <param name="path">The string representation a Path.</param>
        /// <returns></returns>
        public Task<ContentSummary> GetContentSummary(string path)
        {
            return callWebHDFS<ContentSummary>(path, "GETCONTENTSUMMARY", HttpMethod.Get);
        }

        /// <summary>
        /// Get the checksum of a file
        /// </summary>
        /// <param name="path">The file checksum. The default return value is null, which 
        /// indicates that no checksum algorithm is implemented in the corresponding FileSystem. </param>
        /// <returns></returns>
        public Task<FileChecksum> GetFileChecksum(string path)
        {
            return callWebHDFS<FileChecksum>(path, "GETFILECHECKSUM", HttpMethod.Get);
        }

        /// <summary>
        /// Opens an FSDataInputStream at the indicated Path
        /// </summary>
        /// <param name="path">The string representation a Path.</param>
        /// <returns>Async <see cref="Task{Stream}"/> with file content.</returns>
        public async Task<Stream> OpenFile(string path)
        {
            return await OpenFile(path, -1, -1, CancellationToken.None);
        }

        /// <summary>
        /// Opens an FSDataInputStream at the indicated Path
        /// </summary>
        /// <param name="path">The string representation a Path.</param>
        /// <param name="token"><see cref="CancellationToken"/> to cancel call if needed.</param>
        /// <returns>Async <see cref="Task{Stream}"/> with file content.</returns>
        public async Task<Stream> OpenFile(string path, CancellationToken token)
        {
            return await OpenFile(path, -1, -1, token);
        }

        /// <summary>
        /// Opens an FSDataInputStream at the indicated Path.  The offset and length will allow 
        /// you to get a subset of the file.  
        /// </summary>
        /// <param name="path">The string representation a Path.</param>
        /// <param name="offset">The starting byte position. This includes any header bytes</param>
        /// <param name="length">The number of bytes to be processed.</param>
        /// <param name="token"><see cref="CancellationToken"/> to cancel call if needed.</param>
        /// <returns>Async <see cref="Task{Stream}"/> with file content.</returns>
        public async Task<Stream> OpenFile(string path, int offset = 0, int length = -1, CancellationToken token = default(CancellationToken))
        {
            string uri = GetUriForOperation(path).SetQueryParam("op", "OPEN");
            if (offset > 0)
                uri += uri.SetQueryParam("offset", offset.ToString());
            if (length > 0)
                uri += uri.SetQueryParam("length", length.ToString());
            var client = new HttpClient(InnerHandler ?? new HttpClientHandler(), InnerHandler == null);
            var request = new HttpRequestMessage(HttpMethod.Get, uri);
            var response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, token);
            return await response.Content.ReadAsStreamAsync();
        }

        #endregion

        #region "put"
        // todo: add permissions
        /// <summary>
        /// Make the given file and all non-existent parents into directories. 
        /// Has the semantics of Unix 'mkdir -p'. Existence of the directory hierarchy is not an error. 
        /// </summary>
        /// <param name="path">The string representation a Path.</param>
        /// <returns>Async <see cref="Task{Boolean}"/> with result of operation.</returns>
        public async Task<bool> CreateDirectory(string path)
        {
            var result = await callWebHDFS<BooleanResult>(path, "MKDIRS", HttpMethod.Put);
            return result.Value;
        }

        /// <summary>
        /// Renames Path src to Path dst.
        /// </summary>
        /// <param name="path">The string representation a Path.</param>
        /// <param name="newPath"></param>
        /// <returns>Async <see cref="Task{Boolean}"/> with result of operation.</returns>
        public async Task<bool> RenameDirectory(string path, string newPath)
        {
            var result = await callWebHDFS<BooleanResult>(path, "RENAME", HttpMethod.Put, additionalQueryParameters: new { destination = newPath });
            return result.Value;
        }

        /// <summary>
        /// Delete a file.  Note, this will not recursively delete and will
        /// not delete if directory is not empty
        /// </summary>
        /// <param name="path">the path to delete</param>
        /// <returns>Async <see cref="Task{Boolean}"/> with result of operation.</returns>
        public Task<bool> DeleteDirectory(string path)
        {
            return DeleteDirectory(path, false);
        }


        /// <summary>
        /// Delete a file
        /// </summary>
        /// <param name="path">the path to delete</param>
        /// <param name="recursive">if path is a directory and set to true, the directory is deleted else throws an exception.
        /// In case of a file the recursive can be set to either true or false. </param>
        /// <returns>Async <see cref="Task{Boolean}"/> with result of operation.</returns>
        public async Task<bool> DeleteDirectory(string path, bool recursive)
        {
            var result = await callWebHDFS<BooleanResult>(path, "DELETE", HttpMethod.Delete, additionalQueryParameters: new { recursive = recursive.ToString().ToLower() });
            return result.Value;
        }

        /// <summary>
        /// Set permission of a path.
        /// </summary>
        /// <param name="path">The string representation a Path.</param>
        /// <param name="permissions"></param>
        /// <returns>Async <see cref="Task{Boolean}"/> with result of operation.</returns>
        public async Task<bool> SetPermissions(string path, string permissions)
        {
            var result = await callWebHDFS<BooleanResult>(path, "SETPERMISSION", HttpMethod.Put, additionalQueryParameters: new { permission = permissions });
            return result.Value;
        }

        /// <summary>
        /// Sets the owner for the file 
        /// </summary>
        /// <param name="path">The string representation a Path.</param>
        /// <param name="owner">If it is null, the original username remains unchanged</param>
        /// <returns>Async <see cref="Task{Boolean}"/> with result of operation.</returns>
        public async Task<bool> SetOwner(string path, string owner)
        {
            var result = await callWebHDFS<BooleanResult>(path, "SETOWNER", HttpMethod.Put, additionalQueryParameters: new { owner = owner });
            return result.Value;
        }

        /// <summary>
        /// Sets the group for the file 
        /// </summary>
        /// <param name="path">The string representation a Path.</param>
        /// <param name="group">If it is null, the original groupname remains unchanged</param>
        /// <returns>Async <see cref="Task{Boolean}"/> with result of operation.</returns>
        public async Task<bool> SetGroup(string path, string group)
        {
            var result = await callWebHDFS<BooleanResult>(path, "SETOWNER", HttpMethod.Put, additionalQueryParameters: new { group = group });
            return result.Value;
        }

        /// <summary>
        /// Set replication for an existing file.
        /// </summary>
        /// <param name="path">The string representation a Path.</param>
        /// <param name="replicationFactor"></param>
        /// <returns>Async <see cref="Task{Boolean}"/> with result of operation.</returns>
        public async Task<bool> SetReplicationFactor(string path, int replicationFactor)
        {
            var result = await callWebHDFS<BooleanResult>(path, "SETREPLICATION", HttpMethod.Put, additionalQueryParameters: new { replication = replicationFactor });
            return result.Value;
        }

        /// <summary>
        /// Set access time of a file
        /// </summary>
        /// <param name="path">The string representation a Path.</param>
        /// <param name="accessTime">Set the access time of this file. The number of milliseconds since Jan 1, 1970. 
        /// A value of -1 means that this call should not set access time</param>
        /// <returns>Async <see cref="Task{Boolean}"/> with result of operation.</returns>
        public async Task<bool> SetAccessTime(string path, string accessTime)
        {
            var result = await callWebHDFS<BooleanResult>(path, "SETTIMES", HttpMethod.Put, additionalQueryParameters: new { accesstime = accessTime });
            return result.Value;
        }

        /// <summary>
        /// Set modification time of a file
        /// </summary>
        /// <param name="path">The string representation a Path.</param>
        /// <param name="modificationTime">Set the modification time of this file. The number of milliseconds since Jan 1, 1970.
        /// A value of -1 means that this call should not set modification time</param>
        /// <returns>Async <see cref="Task{Boolean}"/> with result of operation.</returns>
        public async Task<bool> SetModificationTime(string path, string modificationTime)
        {
            var result = await callWebHDFS<BooleanResult>(path, "SETTIMES", HttpMethod.Put, additionalQueryParameters: new { modificationtime = modificationTime });
            return result.Value;
        }

        #region CreateFile
        /// <summary>
        /// Opens an FSDataOutputStream at the indicated Path. Files are overwritten by default.
        /// </summary>
        /// <param name="localFile"></param>
        /// <param name="remotePath"></param>
        /// <param name="overwrite"></param>
        /// <param name="token"></param>
        /// <returns>Async <see cref="Task{Boolean}"/> with result of operation.</returns>
        public async Task<bool> CreateFile(string localFile, string remotePath, bool overwrite = false, CancellationToken token = default(CancellationToken))
        {
            var sc = new StreamContent(File.OpenRead(localFile));
            return await createFile(sc, remotePath, overwrite, token);
        }

        /// <summary>
        /// Opens an FSDataOutputStream at the indicated Path. Files are overwritten by default.
        /// </summary>
        /// <param name="content"></param>
        /// <param name="remotePath"></param>
        /// <param name="token"></param>
        /// <returns>Async <see cref="Task{Boolean}"/> with result of operation.</returns>
        public async Task<bool> CreateFile(Stream content, string remotePath, bool overwrite = false, CancellationToken token = default(CancellationToken))
        {
            var sc = new StreamContent(content);
            return await createFile(sc, remotePath, overwrite, token);
        }

        /// <summary>
        /// Opens an FSDataOutputStream at the indicated Path. Files are overwritten by default.
        /// </summary>
        /// <param name="content"></param>
        /// <param name="remotePath"></param>
        /// <param name="token"></param>
        /// <returns>Async <see cref="Task{Boolean}"/> with result of operation.</returns>
        public async Task<bool> CreateFile(byte[] byteArray, string remotePath, bool overwrite = false, CancellationToken token = default(CancellationToken))
        {
            var sc = new ByteArrayContent(byteArray);
            return await createFile(sc, remotePath, overwrite, token);
        }

        private async Task<bool> createFile(HttpContent content, string remotePath, bool overwrite, CancellationToken token)
        {
            var addingUrl = prepareUrl(remotePath, "CREATE", new { overwrite = overwrite.ToString().ToLower() });
            var location = await getRedirectLocation(addingUrl, HttpMethod.Put, token);

            var response = await getResponseMessageAsync(location.ToString(), content, new HttpRequestOptions { Method = HttpMethod.Put, Token = token });
            if (response.StatusCode == System.Net.HttpStatusCode.Created)
            {
                return true;
            }
            else
            {
                throw new HdfsException(response, "File was not created error. See details for plus information.");
            }
        }
        #endregion



        #region AppendFile
        /// <summary>
        /// Opens an FSDataOutputStream at the indicated Path. Files are overwritten by default.
        /// </summary>
        /// <param name="localFile"></param>
        /// <param name="remotePath"></param>
        /// <param name="overwrite"></param>
        /// <param name="token"></param>
        /// <returns>Async <see cref="Task{Boolean}"/> with result of operation.</returns>
        public async Task<bool> AppendFile(string localFile, string remotePath, CancellationToken token = default(CancellationToken))
        {
            var sc = new StreamContent(File.OpenRead(localFile));
            return await AppendFile(sc, remotePath, token);
        }

        /// <summary>
        /// Opens an FSDataOutputStream at the indicated Path. Files are overwritten by default.
        /// </summary>
        /// <param name="content"></param>
        /// <param name="remotePath"></param>
        /// <param name="token"></param>
        /// <returns>Async <see cref="Task{Boolean}"/> with result of operation.</returns>
        public async Task<bool> AppendFile(Stream content, string remotePath, CancellationToken token = default(CancellationToken))
        {
            var sc = new StreamContent(content);
            return await AppendFile(sc, remotePath, token);
        }

        /// <summary>
        /// Opens an FSDataOutputStream at the indicated Path. Files are overwritten by default.
        /// </summary>
        /// <param name="content"></param>
        /// <param name="remotePath"></param>
        /// <param name="token"></param>
        /// <returns>Async <see cref="Task{Boolean}"/> with result of operation.</returns>
        public async Task<bool> AppendFile(byte[] byteArray, string remotePath, CancellationToken token = default(CancellationToken))
        {
            var content = new ByteArrayContent(byteArray);
            return await AppendFile(content, remotePath, token);
        }

        private async Task<bool> AppendFile(HttpContent content, string remotePath, CancellationToken token)
        {
            var addingUrl = prepareUrl(remotePath, "APPEND");
            var location = await getRedirectLocation(addingUrl, HttpMethod.Post, token);

            var response = await getResponseMessageAsync(location.ToString(), content, new HttpRequestOptions { Method = HttpMethod.Post, Token = token });
            if (response.StatusCode == System.Net.HttpStatusCode.OK)
            {
                return true;
            }
            else
            {
                throw new HdfsException(response, "File was not appended error. See details for plus information.");
            }
        }
        #endregion

        ///// <summary>
        ///// Append to an existing file (optional operation).
        ///// </summary>
        ///// <param name="localFile"></param>
        ///// <param name="remotePath"></param>
        ///// <returns></returns>
        //public async Task<string> AppendFile(string localFile, string remotePath)
        //{
        //	var hc = CreateHTTPClient(false);
        //	var resp = await hc.PostAsync(GetUriForOperation(remotePath) + "op=APPEND", null);
        //	if (!resp.IsSuccessStatusCode)
        //		return null;
        //	var postLocation = resp.Headers.Location;
        //	StreamContent sc = new StreamContent(File.OpenRead(localFile));
        //	var resp2 = await hc.PostAsync(postLocation, sc);
        //	if (!resp2.IsSuccessStatusCode)
        //		return null;

        //	// oddly, this is returning a 403 forbidden 
        //	// due to: "IOException","javaClassName":"java.io.IOException","message":"java.io.IOException: 
        //	// Append to hdfs not supported. Please refer to dfs.support.append configuration parameter.
        //	return resp2.Headers.Location.ToString();
        //}

        ///// <summary>
        ///// Append to an existing file (optional operation).
        ///// </summary>
        ///// <param name="localFile"></param>
        ///// <param name="remotePath"></param>
        ///// <returns></returns>
        //public async Task<string> AppendFile(Stream content, string remotePath)
        //{
        //	var hc = CreateHTTPClient(false);
        //	var resp = await hc.PostAsync(GetUriForOperation(remotePath) + "op=APPEND", null);
        //	if (!resp.IsSuccessStatusCode)
        //		return null;

        //	var postLocation = resp.Headers.Location;
        //	var sc = new StreamContent(content);
        //	var resp2 = await hc.PostAsync(postLocation, sc);
        //	if (!resp2.IsSuccessStatusCode)
        //		return null;
        //	return resp2.Headers.Location.ToString();
        //}

        #endregion

        private string GetUriForOperation(string path)
        {
            string uri = BaseUrl.AppendPathSegment(Prefix);
            if (!string.IsNullOrEmpty(path))
            {
                //if (path[0] != '/')
                //{
                //    uri = uri.AppendPathSegment(HomeDirectory);
                //}

                uri = uri.AppendPathSegment(path);
            }

            if (!string.IsNullOrEmpty(User))
                uri = uri.SetQueryParam("user.name", User);

            return uri;
        }

        private Task<T> callWebHDFS<T>(string path, string operation, HttpMethod method, HttpContent content = null, object additionalQueryParameters = null) where T : IJObject, new()
        {
            return callWebHDFS<T>(path, operation, content, new HttpRequestOptions() { Method = method, AdditionalQueryParameters = additionalQueryParameters });
        }

        private async Task<T> callWebHDFS<T>(string path, string operation, HttpContent content = null, HttpRequestOptions options = null) where T : IJObject, new()
        {
            string uri = prepareUrl(path, operation, options.AdditionalQueryParameters);

            var response = await getResponseMessageAsync(uri, content, options ?? new HttpRequestOptions());

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
                    throw new HdfsException(response, "Result Json parsing error", ex);
                }
            }

            return default(T);
        }

        private string prepareUrl(string path, string operation, object additionalQueryParameters = null)
        {
            string uri = GetUriForOperation(path).SetQueryParam("op", operation);

            if (additionalQueryParameters != null)
            {
                uri = uri.SetQueryParams(additionalQueryParameters);
            }

            return uri;
        }

        /// <summary>
        /// Send http request and return <see cref="HttpResponseMessage"/> received.
        /// </summary>
        /// <param name="url">Url to be requested.</param>
        /// <param name="data">Data to be sent.</param>
        /// <param name="options">Request options.</param>
        /// <returns>Asynchronous task.</returns>
        private async Task<HttpResponseMessage> getResponseMessageAsync(string url, object data = null, HttpRequestOptions options = null)
        {
            if (options == null)
                options = new HttpRequestOptions();

            var urib = new UriBuilder(url);
            if (urib.Scheme != "http" && urib.Scheme != "https")
            {
                urib.Scheme = "http";
                url = urib.ToString();
            }

            using (var client = new HttpClient(InnerHandler ?? new HttpClientHandler(), InnerHandler == null))
            {
                client.BaseAddress = new Uri(BaseUrl);

                client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/octet-stream"));

                var request = new HttpRequestMessage(options.Method, url);
                if (options.Method != HttpMethod.Get && data != null)
                {
                    request.Content = data as HttpContent ?? new StringContent(data.ToString(), Encoding.UTF8);
                }

                var response = await client.SendAsync(request, options.Completion, options.Token).ConfigureAwait(false);

                if (!response.IsSuccessStatusCode)
                {
                    throw new HdfsException(response);
                }

                return response;
            }
        }

        private async Task<Uri> getRedirectLocation(string url, HttpMethod method, CancellationToken token = default(CancellationToken))
        {
            using (var client = new HttpClient(new HttpClientHandler() { AllowAutoRedirect = false }, true))
            {
                client.BaseAddress = new Uri(BaseUrl);

                client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/octet-stream"));

                var request = new HttpRequestMessage(method, url);

                var response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, token).ConfigureAwait(false);

                if ((int)response.StatusCode >= 400)
                {
                    throw new HdfsException(response);
                }

                return response.Headers.Location;
            }
        }
    }
}
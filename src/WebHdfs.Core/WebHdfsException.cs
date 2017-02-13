using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace WebHdfs.Core
{
    public class WebHdfsException : Exception
    {
        /// <summary>
        /// <see cref="System.Net.Http.HttpResponseMessage"/> returned by client.
        /// </summary>
        public HttpResponseMessage Response { get; private set; }

        private string _details;

        public WebHdfsException(HttpResponseMessage response, string message, Exception innerException) : this(message, innerException)
        {
            Response = response;

            if (Response != null)
            {
                _details = $"Status and Reason : {(int)Response.StatusCode} {Response.ReasonPhrase}{Environment.NewLine}";

                //try get content
                try
                {
                    var t = Response.Content.ReadAsStringAsync();
                    if (t.Wait(3000))
                    {
                        _details += $"Content : {Regex.Unescape(t.Result)}{Environment.NewLine}";
                    }
                }
                catch { }
            }
        }

        public WebHdfsException(HttpResponseMessage response, string message) : this(response, message, null)
        {
        }

        public WebHdfsException(HttpResponseMessage response) : this(response, null)
        {
        }

        public WebHdfsException(string message, Exception innerException) : base(message, innerException)
        {

        }

        public WebHdfsException()
        {

        }

        public override string Message
        {
            get
            {
                string result = "";
                if (!string.IsNullOrWhiteSpace(base.Message))
                {
                    result += base.Message + Environment.NewLine + Environment.NewLine;
                }

                if (!string.IsNullOrWhiteSpace(_details))
                {
                    result += _details;
                }

                return result;
            }
        }
    }
}

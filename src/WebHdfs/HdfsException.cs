using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;

namespace WebHdfs
{
    public class HdfsException : Exception
    {
        /// <summary>
        /// <see cref="System.Net.Http.HttpResponseMessage"/> returned by client.
        /// </summary>
        public HttpResponseMessage Response { get; private set; }

        private string _content;

        public HdfsException(HttpResponseMessage response, string message, Exception innerException) : base(message, innerException)
        {
            Response = response;

            //try get content
            try
            {
                var t = response.Content.ReadAsStringAsync();
                if (t.Wait(3000))
                {
                    _content = t.Result;
                }
            }
            catch { }
        }

        public HdfsException(HttpResponseMessage response, string message) : this(response, message, null)
        {
        }

        public HdfsException(HttpResponseMessage response) : this(response, null)
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

                if (Response != null)
                {
                    result += $"Status and Reason : {Response.StatusCode} {Response.ReasonPhrase}{Environment.NewLine}";

                    if (!string.IsNullOrWhiteSpace(_content))
                    {
                        result += $"Content : {_content}{Environment.NewLine}";
                    }
                }

                return result;
            }
        }
    }
}

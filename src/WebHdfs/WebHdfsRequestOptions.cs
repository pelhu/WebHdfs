using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace WebHdfs
{
    /// <summary>
    /// Опции запроса
    /// </summary>
    public class WebHdfsRequestOptions
    {
        /// <summary>
        /// дефолтовый конструктор
        /// </summary>
        public WebHdfsRequestOptions()
        {
            Completion = HttpCompletionOption.ResponseContentRead;
            Token = CancellationToken.None;
            Method = HttpMethod.Get;
        }

        public static WebHdfsRequestOptions Default = new WebHdfsRequestOptions();

        /// <summary>
        /// Условие завершения обработки
        /// </summary>
        public HttpCompletionOption Completion { get; set; }

        /// <summary>
        /// Токен для отмены
        /// </summary>
        public CancellationToken Token { get; set; }

        /// <summary>
        /// HTTP-метод
        /// </summary>
        public HttpMethod Method { get; set; }

        /// <summary>
        /// Additional Query Parameters
        /// </summary>
        public object AdditionalQueryParameters { get; set; }
    }
}

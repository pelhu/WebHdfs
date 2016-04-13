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
    public class HttpRequestOptions
    {
        /// <summary>
        /// дефолтовый конструктор
        /// </summary>
        public HttpRequestOptions()
        {
            Completion = HttpCompletionOption.ResponseContentRead;
            Token = CancellationToken.None;
            //MediaType = "application/json";
            Method = HttpMethod.Get;
        }

        /// <summary>
        /// Условие завершения обработки
        /// </summary>
        public HttpCompletionOption Completion
        { get; set; }

        /// <summary>
        /// Токен для отмены
        /// </summary>
        public CancellationToken Token
        { get; set; }

        ///// <summary>
        ///// Форматировщик тела запроса
        ///// </summary>
        //public string MediaType
        //{ get; set; }

        /// <summary>
        /// HTTP-метод
        /// </summary>
        public HttpMethod Method
        { get; set; }

        /// <summary>
        /// Additional Query Parameters
        /// </summary>
        public object AdditionalQueryParameters { get; set; }
    }
}

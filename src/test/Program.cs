using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using WebHdfs;

namespace test
{
    public class Program
    {
        public static void Main(string[] args)
        {
            testHdfs().Wait();
        }

        private async static Task testHdfs()
        {
            var w = new WebHdfsClient("http://172.17.7.211:50070", "root");

            var ds = await w.CreateDirectory("khamzat_test");

            await w.CreateFile(@"c:\!temp\build2.txt", "khamzat_test/build2.txt");
            //await w.CreateFile(@"c:\!temp\syslog8", "khamzat_test/syslog8");
        }
    }
}

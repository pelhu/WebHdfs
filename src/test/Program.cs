using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using WebHdfs.Core;

namespace test
{
    public class Program
    {
        public static void Main(string[] args)
        {
            //var w = GetW();
            //var result = w.GetContentSummary("khamzat_test2").Result;
            //Console.WriteLine(JsonConvert.SerializeObject(result));

            //Console.ReadKey();
            //return;

            var w = GetW();

            w.CreateFileAsync("test", Encoding.UTF8, "/start-yarn.cmd").Wait();
            w.DeleteAsync("/start-yarn.cmd").Wait();
            w.CreateFileAsync("test", Encoding.UTF8, "/start-yarn.cmd").Wait();
            w.DeleteAsync("/start-yarn.cmd").Wait();

            Console.ReadKey();
            return;

            //var w = GetW();
            //var result = w.Delete("khamzat_test", true).Result;
            //Console.WriteLine(JsonConvert.SerializeObject(result));

            //Console.ReadKey();
            //return;


            //for (int i = 0; i < args.Length; i++)
            //{
            //    Console.WriteLine($"arg {i} : {args[i]}");
            //}

            //Console.ReadKey();

            //return;

            //var a = GetW().UploadDirectory(@"C:\!temp\testHdfs", "testHdfs", true).Result;

            //testCreate().Wait();
            //testAppend().Wait();
            //testAppendFile().Wait();
            //testRead().Wait();

            Console.ReadKey();
        }

        private async static Task testRead()
        {
            var w = GetW();

            var ds = await w.GetDirectoryItemsAsync("khamzat_test2");

            var inputPath = @"c:\!temp\testHdfs\";

            Directory.GetFiles(inputPath).ToList().ForEach(f => File.Delete(f));

            var sw = new Stopwatch();
            var tasks = new List<Task>();
            sw.Start();
            foreach (var item in ds.Files)
            {
                tasks.Add(Task.Run(async () =>
                {
                    using (var r = await w.OpenFileReadAsync("khamzat_test2/" + item.PathSuffix))
                    {
                        using (var f = File.OpenWrite(Path.Combine(inputPath, item.PathSuffix)))
                        {
                            await r.CopyToAsync(f);
                        }
                    }
                }));
            }
            Task.WaitAll(tasks.ToArray());
            sw.Stop();
            var totalSize = ds.Files.Sum(i => i.Length);
            var elapsedMilliseconds = sw.ElapsedMilliseconds;
            Console.WriteLine($"Total sended : {totalSize.ToString("n0")} in {elapsedMilliseconds.ToString("n0")} milliseconds. Average speed : {(totalSize / (elapsedMilliseconds / 1000M)).ToString("n0")} bytes/second");
        }

        private static WebHdfsClient GetW()
        {
            return new WebHdfsClient("http://172.17.6.59:50070", "root");
        }

        private async static Task testAppend()
        {
            var w = GetW();

            var ds = await w.GetDirectoryItemsAsync("khamzat_test2");

            var byteArrayLength = 10000000;

            var sw = new Stopwatch();
            var tasks = new List<Task>();
            sw.Start();
            foreach (var item in ds.Files)
            {
                tasks.Add(w.AppendFileAsync(new byte[byteArrayLength], "khamzat_test2/" + item.PathSuffix));
            }
            Task.WaitAll(tasks.ToArray());
            sw.Stop();
            var totalSize = byteArrayLength * ds.Files.Count();
            var elapsedMilliseconds = sw.ElapsedMilliseconds;
            Console.WriteLine($"Total sended : {totalSize.ToString("n0")} in {elapsedMilliseconds.ToString("n0")} milliseconds. Average speed : {(totalSize / (elapsedMilliseconds / 1000M)).ToString("n0")} bytes/second");


        }

        private async static Task testAppendFile()
        {
            var sw = new Stopwatch();
            var tasks = new List<Task>();
            sw.Start();

            var w = GetW();

            var ds = await w.GetDirectoryItemsAsync("khamzat_test2");

            var fileInfo = new FileInfo(@"c:\!temp\dotnet-apiport-master.rar");

            foreach (var item in ds.Files)
            {
                tasks.Add(w.AppendFileAsync(fileInfo.FullName, "khamzat_test2/" + item.PathSuffix));
            }
            Task.WaitAll(tasks.ToArray());
            sw.Stop();
            var totalSize = fileInfo.Length * ds.Files.Count();
            var elapsedMilliseconds = sw.ElapsedMilliseconds;
            Console.WriteLine($"Total sended : {totalSize.ToString("n0")} in {elapsedMilliseconds.ToString("n0")} milliseconds. Average speed : {(totalSize / (elapsedMilliseconds / 1000M)).ToString("n0")} bytes/second");


        }

        private async static Task testCreate()
        {
            var w = GetW();
            
            var fileInfo = new FileInfo(@"c:\!temp\syslog8");

            var ds = await w.GetDirectoryItemsAsync("khamzat_test2");
            if (ds!=null)
            {
                foreach (var item in ds.Files.Where(f => f.PathSuffix.StartsWith(fileInfo.Name)))
                {
                    await w.DeleteAsync("khamzat_test2/" + item.PathSuffix);
                }
            }

            var count = 3;

            var sw = new Stopwatch();
            //var tasks = new List<Task>();
            sw.Start();
            for (int i = 0; i < count; i++)
            {
                await w.UploadFileAsync(fileInfo.FullName, $"khamzat_test2/{fileInfo.Name}_{(i + 1)}", true);
                //tasks.Add(w.CreateFile(fileInfo.FullName, $"khamzat_test2/{fileInfo.Name}_{(i + 1)}", true));

                //w.CreateFile(fileInfo.FullName, $"khamzat_test/{fileInfo.Name}_{(i + 1)}", true).ContinueWith(t =>
                //  {
                //      var totalSize = fileInfo.Length * (i + 1);
                //      var elapsedMilliseconds = sw.ElapsedMilliseconds;
                //      Console.WriteLine(t.Result);
                //      Console.WriteLine($"Total sended : {totalSize.ToString("n0")} in {elapsedMilliseconds.ToString("n0")} milliseconds. Average speed : {(totalSize / (elapsedMilliseconds / 1000M)).ToString("n0")} bytes/second");

                //  }).Wait(50);
            }

            //Task.WaitAll(tasks.ToArray());

            sw.Stop();
            var totalSize = fileInfo.Length * count;
            var elapsedMilliseconds = sw.ElapsedMilliseconds;
            Console.WriteLine($"Total sended : {totalSize.ToString("n0")} in {elapsedMilliseconds.ToString("n0")} milliseconds. Average speed : {(totalSize / (elapsedMilliseconds / 1000M)).ToString("n0")} bytes/second");

        }
    }
}

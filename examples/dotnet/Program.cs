using System;
using System.IO;
using System.Text;
using Google.Cloud.Storage.V1;

var client = new StorageClientBuilder
{
    // storage client for .net will only work with http hosted emulator
    // also specifying -external-url option is required for the uploads to function properly
    // see attached docker-compose.yaml file for details
    BaseUri = "http://localhost:8080/storage/v1/",
    UnauthenticatedAccess = true
}.Build();

Console.WriteLine("Creating bucket");
client.CreateBucket("test-project", "test-bucket");

Console.WriteLine("Uploading object");
client.UploadObject("test-bucket", "hello.txt", "text/plain", 
    new MemoryStream(Encoding.UTF8.GetBytes("Hello Google Storage")));
    
Console.WriteLine("Downloading object");
var ms = new MemoryStream();
client.DownloadObject("test-bucket", "hello.txt", ms);

Console.WriteLine("Downloaded: {0}", Encoding.UTF8.GetString(ms.ToArray()));
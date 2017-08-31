using BlockSample;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BlobsSample
{
    class Program
    {
        const string connectionstring = "replace with storage accounty connection string";
        const string StorageAccount = "replae with storage account";
        const string SASToken = "replace with SAS token for file";

        static void Main(string[] args)
        {
            int oneKB = 1024; //1 kb
            int blockSize = 512; //kb
            int nrBlocks = 10;
            int nrDirectories = 5;
            int nrSubFolders = 1;
            int nrFilesPerDirectory = 10;

            //Create Folder Structure
            CreateFolderStructure(oneKB, blockSize, nrDirectories, nrSubFolders, nrFilesPerDirectory);

            //Show Folder Structure and list files in each directory
            ShowFolderStructure("testappendblobs");

            //create a new blob to append to.
            var blob = new AppendBlob("testappendblobs", "appendfile.bin", connectionstring);

            //Write To an Append Blob
            WriteToAppendBlob(oneKB, blockSize, nrBlocks, blob);

            //Show Properties from Blob
            ReadPropertiesFromBlob(blob);

            //Read bytes from AppendBlob
            ReadBytesFromAppendBlob(oneKB, blockSize, blob);


            //Delete the blob
            DeleteAppendBlob(blob);

            Console.WriteLine("Press any key");
            Console.ReadLine();
        }

        private static void DeleteAppendBlob(AppendBlob blob)
        {
            Console.WriteLine("Delete the blob");
            blob.Remove();
        }

        private static void ShowFolderStructure(string containerName)
        {
            var container = new CloudBlobContainer(new Uri($"{StorageAccount}/{containerName}/{SASToken}"));

            // Pass Ienumerable to recursive function to get "subdirectories":
            Console.WriteLine(getContainerDirectories(container.ListBlobs(), " "));
        }

        /// <summary>
        /// Code sample From https://www.codeproject.com/articles/597939/modelingplusaplusdirectoryplusstructureplusonplusa
        /// </summary>
        /// <param name="blobList"></param>
        /// <param name="indent"></param>
        /// <returns></returns>
        static string getContainerDirectories(IEnumerable<IListBlobItem> blobList, string indent)
        {
            // Indent each item in the output for the current subdirectory:
            indent = indent + "  ";
            StringBuilder sb = new StringBuilder("");

            // First list all the actual FILES within 
            // the current blob list. No recursion needed:
            foreach (var item in blobList.Where((blobItem, type) => blobItem is CloudAppendBlob || blobItem is CloudBlockBlob))
            {
                var blobFile = item as CloudBlob;
                sb.AppendLine(indent + blobFile.Name + " ; " + blobFile.Properties.LastModified.ToString() + " ; parent=" + blobFile.Parent.Prefix);
            }

            // List all additional subdirectories 
            // in the current directory, and call recursively:
            foreach (var item in blobList.Where
            ((blobItem, type) => blobItem is CloudBlobDirectory))
            {
                var directory = item as CloudBlobDirectory;
                sb.AppendLine(indent + directory.Prefix.ToUpper());

                // Call this method recursively to retrieve subdirectories within the current:
                sb.AppendLine(getContainerDirectories(directory.ListBlobs(), indent));
            }
            return sb.ToString();
        }

        private static void ReadPropertiesFromBlob(AppendBlob blob)
        {
            var filesizetask = blob.GetBlobProperties();
            var fileProperties = filesizetask.Result;

            Console.WriteLine($"File is of type {fileProperties.BlobType} and has length {fileProperties.Length / 1024} kb and last modified at {fileProperties.LastModified}.");
        }

        private static void ReadBytesFromAppendBlob(int oneKB, int blockSize, AppendBlob blobStream)
        {
            Console.WriteLine("/n/n");
            Console.WriteLine("Start reading a byte at every 1024 bytes");


            var filesizetask = blobStream.GetBlobProperties();
            var fileProperties = filesizetask.Result;

            for (int b = 0; b < fileProperties.Length; b += blockSize * oneKB)
            {
                byte[] bytesArray = null;
                var bytesRead = blobStream.Read(out bytesArray, b, oneKB);

                Console.WriteLine($"First 5 byte from position {b} had value {bytesArray[0]},{bytesArray[1]},{bytesArray[2]},{bytesArray[3]},{bytesArray[4]}");
            }
        }

        private static void WriteToAppendBlob(int oneKB, int blockSize, int nrBlocks, AppendBlob blobStream)
        {
            Console.WriteLine("Writing data to test2.bin./n/n");
            Console.WriteLine($"nrBlocks;blockSize;Seconds;kbits/sec");

            var t0 = DateTime.Now;

            for (int i = 0; i < nrBlocks; i++)
            {
                var bytes = GetRandomBytes(blockSize * oneKB);
                blobStream.Write(bytes, 0, bytes.Length);
            }
            var t1 = DateTime.Now;

            var delta = t1 - t0;
            Console.WriteLine($"{nrBlocks};{blockSize};{delta.TotalSeconds};{(blockSize * 8 * nrBlocks) / delta.TotalSeconds}");
        }

        private static void CreateFolderStructure(int oneKB, int blockSize, int nrDirectories, int nrSubFolders, int nrFilesPerDirectory)
        {
            for (var d = 0; d < nrDirectories; d++)
            {
                for (var f = 0; f < nrFilesPerDirectory; f++)
                {
                    var filename = $"Folder{d}/file{f}.bin";
                    var bStream = new AppendBlob("testappendblobs", filename, connectionstring);
                    var oneKb = GetRandomBytes(blockSize * oneKB);
                    bStream.Write(oneKb, 0, oneKb.Length);

                }

                for (var s = 0; s < nrSubFolders; s++)
                {
                    for (var f = 0; f < nrFilesPerDirectory; f++)
                    {
                        var filename = $"Folder{d}/Folder{d}-{s}/file{f}.bin";
                        var bStream = new AppendBlob("testappendblobs", filename, connectionstring);
                        var oneKb = GetRandomBytes(blockSize * oneKB);
                        bStream.Write(oneKb, 0, oneKb.Length);
                    }
                }
            }
        }

        static byte[] GetRandomBytes(int size)
        {
            Random rnd = new Random();
            Byte[] b = new Byte[size];
            rnd.NextBytes(b);
            return b;
        }

    }
}

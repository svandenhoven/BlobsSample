using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System.IO;
using System.Threading.Tasks;
using System;

namespace BlockSample
{
    class AppendBlob
    {
        private CloudAppendBlob blob;
        private long position;

        public AppendBlob(string containerName, string fileName, string connectionstring)
        {
            var storageAccount = CloudStorageAccount.Parse(connectionstring);
            var client = storageAccount.CreateCloudBlobClient();

            var container = client.GetContainerReference(containerName);
            blob = container.GetAppendBlobReference(fileName);
            blob.CreateOrReplaceAsync().Wait();
        }


        public long Length
        {
            get
            {
                var task = GetBlobProperties();
                var length = task.Result.Length;
                return length;
            }
        }

        public long Position
        {
            get
            {
                return position;
            }
            set
            {
                position = value;
            }
        }


        public int Read(out byte[] buffer, int offset, int count)
        {
            var bytes = new byte[count];
            var task = blob.DownloadRangeToByteArrayAsync(bytes, 0, offset, count);
            buffer = bytes;

            return task.Result;
        }

        public long Seek(long offset, SeekOrigin origin)
        {
            var task = GetBlobProperties();
            var prop = task.Result;
            if (offset <= prop.Length)
            {
                Position = offset;
                return Position;
            }
            else
            {
                return -1;
            }
        }

        internal void Remove()
        {
            blob.Delete();
        }

        /// <summary>
        /// Delivers blob information like length, lastmodified, etag
        /// </summary>
        /// <returns></returns>
        public async Task<BlobProperties> GetBlobProperties()
        {
            if (await blob.ExistsAsync())
            {
                await blob.FetchAttributesAsync();
                return blob.Properties;
            }

            return null;
        }

        public void Rename(string newName)
        {

        }

        public void Write(byte[] buffer, int offset, int count)
        {
            var stream = new MemoryStream(buffer, 0, count);
            var t = blob.AppendFromStreamAsync(stream);
            t.Wait();
        }
    }
}

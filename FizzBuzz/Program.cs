using System;
using System.Buffers;
using System.IO;
using System.IO.Compression;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace FizzBuzz
{
    class Program
    {
        static Task Main(string[] args)
        {
            var pipe = new Pipe(new PipeOptions(readerScheduler: PipeScheduler.ThreadPool,
                writerScheduler: PipeScheduler.ThreadPool, useSynchronizationContext: false, 
                pool: MemoryPool<byte>.Shared, pauseWriterThreshold: 1024 * 32, resumeWriterThreshold: 1024 * 16));
            var brotliPipe = new Pipe(new PipeOptions(readerScheduler: PipeScheduler.ThreadPool,
                writerScheduler: PipeScheduler.ThreadPool, useSynchronizationContext: false,
                pool: MemoryPool<byte>.Shared, pauseWriterThreshold: 1024 * 1024 * 128, resumeWriterThreshold: 1024 * 16));

            var fizzbuzz = FillPipeAsync(pipe.Writer);
            var compressing = BrotliCompressAsync(pipe.Reader, brotliPipe.Writer);
            var writingToFile = WriteToFileAsync(brotliPipe.Reader, "result.brotli");

            return Task.WhenAll(fizzbuzz, compressing, writingToFile);
        }

        private static async Task FillPipeAsync(PipeWriter writer)
        {
            // result file format: 
            // | flag (1byte) | data (8 bytes) |
            // flag: none = 0x00, fizz = 0x01, buzz = 0x02, fizzbuzz = 0x03
            const int blockSize = 9;
            var block = new byte[blockSize];
            void WriteFizzBuzz(ulong x)
            {
                block[0] = (byte) x.GetFizzBuzz();
                if (!BitConverter.TryWriteBytes(block.AsSpan(1), x))
                {
                    throw new Exception();
                }
                block.CopyTo(writer.GetSpan(blockSize));
                writer.Advance(blockSize);
            }

            for (ulong i = 0; i < ulong.MaxValue; i++)
            {
                WriteFizzBuzz(i);
                if (i % 128 == 0) await writer.FlushAsync();
                if (i % 1000000 == 0) Console.WriteLine(i);
            }

            await writer.FlushAsync();
            writer.Complete();
        }

        private static async Task BrotliCompressAsync(PipeReader rawReader, PipeWriter writer)
        {
            using (var brotli = new BrotliEncoder(1, 24))
            {
                while (true)
                {
                    var result = await rawReader.ReadAsync();

                    if (result.Buffer.Length > int.MaxValue)
                    {
                        throw new Exception();
                    }

                    if (!result.Buffer.IsEmpty)
                    {
                        using (var sourceMemoryOwner = MemoryPool<byte>.Shared.Rent((int) result.Buffer.Length))
                        using (var destMemoryOwner = MemoryPool<byte>.Shared.Rent((int) result.Buffer.Length))
                        {
                            var sourceMemory = sourceMemoryOwner.Memory.Slice(0, (int) result.Buffer.Length);
                            result.Buffer.CopyTo(sourceMemory.Span);

                            brotli.Compress(sourceMemory.Span, destMemoryOwner.Memory.Span, out var bytesConsumed,
                                out var bytesWritten, result.IsCompleted);

                            rawReader.AdvanceTo(result.Buffer.GetPosition(bytesConsumed));

                            var destMemory = destMemoryOwner.Memory.Slice(0, bytesWritten);
                            await writer.WriteAsync(destMemory);
                        }
                    }

                    if (result.IsCompleted)
                    {
                        break;
                    }
                }

                rawReader.Complete();
                writer.Complete();
            }
        }

        private static async Task WriteToFileAsync(PipeReader reader, string filePath)
        {
            using (var fileStream =
                new FileStream(filePath, FileMode.Create, FileAccess.Write, FileShare.Read, 1024))
            {
                while (true)
                {
                    var result = await reader.ReadAsync();

                    if (result.Buffer.Length > int.MaxValue)
                    {
                        throw new Exception();
                    }

                    if (!result.Buffer.IsEmpty)
                    {
                        using (var memoryOwner = MemoryPool<byte>.Shared.Rent((int) result.Buffer.Length))
                        {
                            var memory = memoryOwner.Memory.Slice(0, (int) result.Buffer.Length);
                            result.Buffer.CopyTo(memory.Span);
                            await fileStream.WriteAsync(memory);
                        }
                    }
                    else
                    {
                        Console.WriteLine("w: empty!");
                    }

                    if (result.Buffer.IsEmpty && result.IsCompleted)
                    {
                        break;
                    }

                    reader.AdvanceTo(result.Buffer.End);
                }
            }

            reader.Complete();
        }
    }
    
    internal static class FizzBuzzPolicy
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static FizzBuzzFlag GetFizzBuzz(this in ulong x)
        {
            var result = FizzBuzzFlag.None;
            if (x % 3 == 0) result |= FizzBuzzFlag.Fizz;
            if (x % 5 == 0) result |= FizzBuzzFlag.Buzz;
            return result;
        } 
    }

    [Flags]
    public enum FizzBuzzFlag : byte
    {
        None = 0x00,
        Fizz = 0x01,
        Buzz = 0x02,
        FizzBuzz = Fizz | Buzz
    }
}
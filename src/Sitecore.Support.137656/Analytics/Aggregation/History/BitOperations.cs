using System;
using System.Linq;
using Sitecore.Diagnostics;

namespace Sitecore.Support.Analytics.Aggregation.History
{
  internal static class BitOperations
  {
    public static int GetBitsForN(long n)
    {
      var num = (ulong) n;
      var num2 = 0;
      while (num > 0L)
      {
        num = num >> 1;
        num2++;
      }
      return num2;
    }

    public static int GetBytesForBits(int bits) =>
      (bits >> 3) + ((bits & 7) > 0 ? 1 : 0);

    public static byte[] MaskBitsExcept(byte[] data, int bits)
    {
      Assert.ArgumentNotNull(data, "data");
      var length = bits >> 3;
      if (length > data.Length)
        throw new InvalidOperationException("Number of bits to keep is more than number of bits in the array.");
      var destinationArray = new byte[data.Length];
      Array.Copy(data, destinationArray, length);
      var num2 = (byte) (bits & 7);
      if (num2 > 0)
      {
        if (length == data.Length)
          throw new InvalidOperationException("Number of bits to keep is more than number of bits in the array.");
        var index = length;
        var num4 = (byte) (0xff << (8 - num2));
        destinationArray[index] = (byte) (data[index] & num4);
      }
      return destinationArray;
    }

    public static byte[] RaiseBitsExcept(byte[] data, byte bits)
    {
      Assert.ArgumentNotNull(data, "data");
      Assert.ArgumentNotNull(data, "data");
      var length = bits >> 3;
      if (length > data.Length)
        throw new InvalidOperationException("Number of bits to keep is more than number of bits in the array.");
      var destinationArray = Enumerable.Repeat<byte>(0xff, data.Length).ToArray();
      Array.Copy(data, destinationArray, length);
      var num2 = (byte) (bits & 7);
      if (num2 > 0)
      {
        if (length == data.Length)
          throw new InvalidOperationException("Number of bits to keep is more than number of bits in the array.");
        var index = length;
        var num4 = (byte) (0xff >> num2);
        destinationArray[index] = (byte) (data[index] | num4);
      }
      return destinationArray;
    }

    public static long ReadInt64BigEndian(byte[] array, int startIndex)
    {
      Assert.ArgumentNotNull(array, "array");
      var num = 0L;
      for (var i = 0; i < 8; i++)
        num = (num << 8) | array[i + startIndex];
      return num;
    }

    public static void WriteInt64BigEndian(long value, byte[] array, int startIndex)
    {
      Assert.ArgumentNotNull(array, "array");
      for (var i = 7; i >= 0; i--)
      {
        array[i + startIndex] = (byte) (value & 0xffL);
        value = value >> 8;
      }
    }
  }
}
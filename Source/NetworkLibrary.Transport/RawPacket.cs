using System.Buffers;

namespace NetworkLibrary.Transport
{
    /// <summary>
    /// Represents a raw UDP datagram pulled from the socket.
    /// Used to pass data between the dedicated Receive Thread and the Main Thread.
    /// </summary>
    public struct RawPacket
    {
        public byte[] Buffer;
        public int Length;
        public PeerAddress Sender;
    }
}

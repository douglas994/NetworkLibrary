using NetworkLibrary.Serialization;

namespace NetworkLibrary.Packets
{
    /// <summary>
    /// Wrapper around BitBuffer for reading packet data.
    /// Stack-allocated to avoid GC pressure.
    /// </summary>
    public ref struct PacketReader
    {
        private ref BitBuffer _buffer;

        public PacketReader(ref BitBuffer buffer)
        {
            _buffer = ref buffer;
        }

        public bool ReadBool() => _buffer.ReadBool();
        public byte ReadByte() => _buffer.ReadByte();
        public ushort ReadUShort() => _buffer.ReadUShort();
        public uint ReadUInt() => _buffer.ReadUInt();
        public int ReadInt() => _buffer.ReadInt();
        public float ReadFloat() => _buffer.ReadFloat();
        public string ReadString() => _buffer.ReadString();
        public int ReadIntVar() => _buffer.ReadIntVar();
        public uint ReadUIntVar() => _buffer.ReadUIntVar();
        public float ReadCompressedFloat(float min, float max, float precision) => _buffer.ReadCompressedFloat(min, max, precision);
    }
}

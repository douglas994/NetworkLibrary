using NetworkLibrary.Serialization;

namespace NetworkLibrary.Packets
{
    /// <summary>
    /// Wrapper around BitBuffer for writing packet data.
    /// Stack-allocated to avoid GC pressure.
    /// </summary>
    public ref struct PacketWriter
    {
        private ref BitBuffer _buffer;

        public PacketWriter(ref BitBuffer buffer)
        {
            _buffer = ref buffer;
        }

        public void WriteBool(bool value) => _buffer.AddBool(value);
        public void WriteByte(byte value) => _buffer.AddByte(value);
        public void WriteUShort(ushort value) => _buffer.AddUShort(value);
        public void WriteUInt(uint value) => _buffer.AddUInt(value);
        public void WriteInt(int value) => _buffer.AddInt(value);
        public void WriteFloat(float value) => _buffer.AddFloat(value);
        public void WriteString(string value) => _buffer.AddString(value);
        public void WriteIntVar(int value) => _buffer.AddIntVar(value);
        public void WriteUIntVar(uint value) => _buffer.AddUIntVar(value);
        public void WriteCompressedFloat(float min, float max, float precision, float value) => _buffer.AddCompressedFloat(min, max, precision, value);
    }
}

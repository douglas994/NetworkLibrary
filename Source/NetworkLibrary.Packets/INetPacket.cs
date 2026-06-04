namespace NetworkLibrary.Packets
{
    /// <summary>
    /// Contract for defining a zero-allocation network packet.
    /// Packets should be defined as structs.
    /// </summary>
    public interface INetPacket
    {
        void Serialize(ref PacketWriter writer);
        void Deserialize(ref PacketReader reader);
    }
}

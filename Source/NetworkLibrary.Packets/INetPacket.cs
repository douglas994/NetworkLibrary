using NetworkLibrary.Serialization;

namespace NetworkLibrary.Packets
{
    /// <summary>
    /// Contract for defining a zero-allocation network packet.
    /// Packets should be defined as structs.
    /// </summary>
    public interface INetPacket
    {
        void Serialize(ref BitBuffer writer);
        void Deserialize(ref BitBuffer reader);
    }
}

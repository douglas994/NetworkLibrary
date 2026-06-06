using System;
using System.Collections.Generic;
using NetworkLibrary.Serialization;

namespace NetworkLibrary.Packets
{
    /// <summary>
    /// Registry for packet handlers. Dispatches incoming bytes to struct-based handlers automatically.
    /// </summary>
    public sealed class PacketDispatcher
    {
        private delegate void PacketHandlerDelegate(NetPeer peer, ref BitBuffer buffer);
        private readonly Dictionary<byte, PacketHandlerDelegate> _handlers = new Dictionary<byte, PacketHandlerDelegate>();

        /// <summary>
        /// Subscribes a handler for a specific packet ID and struct type.
        /// </summary>
        public void Subscribe<T>(byte packetId, Action<NetPeer, T> handler) where T : struct, INetPacket
        {
            _handlers[packetId] = (NetPeer peer, ref BitBuffer buffer) =>
            {
                T packet = new T();
                packet.Deserialize(ref buffer);
                handler(peer, packet);
            };
        }

        /// <summary>
        /// Reads the packet ID and dispatches the payload to the registered handler.
        /// </summary>
        public void Dispatch(NetPeer peer, ref BitBuffer buffer)
        {
            if (buffer.BitsAvailable < 8) return; // Need at least 1 byte for ID
            
            byte packetId = buffer.ReadByte();
            if (_handlers.TryGetValue(packetId, out var handler))
            {
                handler(peer, ref buffer);
            }
        }
    }
}

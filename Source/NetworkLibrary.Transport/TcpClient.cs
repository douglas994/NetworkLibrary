/*
 *  NetworkLibrary - High-Performance Networking for MMORPGs
 *  Module: Transport - TcpClient
 *
 *  Abstracted and easy-to-use TCP Client.
 *  Uses Background Threads for I/O and MPMC Queue for Main-Thread updates.
 */

using System;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using NetworkLibrary.Serialization;

namespace NetworkLibrary.Transport
{
    /// <summary>
    /// Advanced, High-Performance TCP Client designed for Unity and MMORPGs.
    /// All network events are dispatched safely to the main thread when you call Update().
    /// </summary>
    public sealed class TcpClient : IDisposable
    {
        private TcpPeer? _peer;
        private bool _isConnecting;
        
        // Thread-safe lock-free queue for bringing background events to the main thread
        private readonly ConcurrentQueue<TcpEvent> _eventQueue;

        /// <summary>Called on the main thread when successfully connected to the server.</summary>
        public Action? OnConnected;

        /// <summary>Called on the main thread when disconnected from the server.</summary>
        public Action? OnDisconnected;

        /// <summary>Called on the main thread when a complete message is received.</summary>
        /// <remarks>The byte array passed is ready to be loaded into a BitBuffer via buffer.FromSpan(new ReadOnlySpan&lt;byte&gt;(data, offset, length))</remarks>
        public Action<byte[], int, int>? OnDataReceived;

        /// <summary>Returns true if currently connected to a server.</summary>
        public bool IsConnected => _peer != null && _peer.IsConnected;

        public TcpClient()
        {
            _eventQueue = new ConcurrentQueue<TcpEvent>();
        }

        /// <summary>
        /// Connects to a server asynchronously.
        /// </summary>
        public void Connect(string host, int port)
        {
            if (IsConnected || _isConnecting) return;
            _isConnecting = true;

            Task.Run(() =>
            {
                try
                {
                    var addresses = Dns.GetHostAddresses(host);
                    if (addresses.Length == 0) throw new Exception("Host not found");

                    var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                    socket.NoDelay = true;
                    
                    socket.Connect(addresses[0], port);

                    // Successfully connected
                    _peer = new TcpPeer(socket, 0, OnMessageReceivedFromPeer, OnPeerDisconnectedCallback);
                    
                    _eventQueue.Enqueue(new TcpEvent { Type = TcpEventType.Connected });

                    _isConnecting = false;
                    _peer.StartReceiving();
                }
                catch (Exception)
                {
                    _isConnecting = false;
                    _eventQueue.Enqueue(new TcpEvent { Type = TcpEventType.Disconnected });
                }
            });
        }

        /// <summary>
        /// Disconnects from the server gracefully.
        /// </summary>
        public void Disconnect()
        {
            _peer?.Disconnect();
            _peer = null;
        }

        /// <summary>
        /// MÁGICA: Call this method exactly once per frame (e.g., Unity's Update() method).
        /// This ensures all events (connections, data, disconnects) run on the Main Thread safely!
        /// </summary>
        public void Update()
        {
            while (_eventQueue.TryDequeue(out TcpEvent ev))
            {
                switch (ev.Type)
                {
                    case TcpEventType.Connected:
                        OnConnected?.Invoke();
                        break;

                    case TcpEventType.Disconnected:
                        _peer = null;
                        OnDisconnected?.Invoke();
                        break;

                    case TcpEventType.DataReceived:
                        OnDataReceived?.Invoke(ev.Data!, ev.Offset, ev.Length);
                        if (ev.Data != null)
                        {
                            System.Buffers.ArrayPool<byte>.Shared.Return(ev.Data);
                        }
                        break;
                }
            }
        }

        /// <summary>
        /// Sends a message to the server via BitBuffer.
        /// </summary>
        public void Send(BitBuffer buffer)
        {
            _peer?.Send(buffer);
        }

        /// <summary>
        /// Sends raw bytes to the server.
        /// </summary>
        public void Send(byte[] data, int offset, int length)
        {
            _peer?.Send(data, offset, length);
        }

        // ════════════════════════════════════════════════════════
        // INTERNAL I/O (Runs on Background Threads)
        // ════════════════════════════════════════════════════════

        private void OnMessageReceivedFromPeer(TcpPeer peer, byte[] rawData, int offset, int length)
        {
            // Rent array from ArrayPool to prevent GC allocation
            byte[] msgData = System.Buffers.ArrayPool<byte>.Shared.Rent(length);
            Buffer.BlockCopy(rawData, offset, msgData, 0, length);

            _eventQueue.Enqueue(new TcpEvent
            {
                Type = TcpEventType.DataReceived,
                Data = msgData,
                Offset = 0,
                Length = length
            });
        }

        private void OnPeerDisconnectedCallback(TcpPeer peer)
        {
            _eventQueue.Enqueue(new TcpEvent { Type = TcpEventType.Disconnected });
        }

        public void Dispose()
        {
            Disconnect();
        }
    }
}

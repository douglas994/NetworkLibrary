/*
 *  NetworkLibrary - High-Performance Networking for MMORPGs
 *  Module: Transport - TcpServer
 *
 *  Abstracted and easy-to-use TCP Server.
 *  Uses Background Threads for I/O and MPMC Queue for Main-Thread updates.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using NetworkLibrary.Serialization;

namespace NetworkLibrary.Transport
{
    /// <summary>
    /// Event type for routing background network actions to the main game thread.
    /// </summary>
    internal enum TcpEventType : byte
    {
        Connected,
        Disconnected,
        DataReceived
    }

    /// <summary>
    /// Structure representing a queued network event.
    /// </summary>
    internal struct TcpEvent
    {
        public TcpEventType Type;
        public TcpPeer Peer;
        public byte[]? Data;
        public int Offset;
        public int Length;
    }

    /// <summary>
    /// Advanced, High-Performance TCP Server designed for Unity and MMORPGs.
    /// All network events are dispatched safely to the main thread when you call Update().
    /// </summary>
    public sealed class TcpServer : IDisposable
    {
        private Socket? _listenerSocket;
        private bool _isRunning;
        private uint _nextPeerId = 1;

        private readonly ConcurrentDictionary<uint, TcpPeer> _peers;
        
        // Thread-safe lock-free queue for bringing background events to the main thread
        private readonly ConcurrentQueue<TcpEvent> _eventQueue;

        /// <summary>Called on the main thread when a client connects.</summary>
        public Action<TcpPeer>? OnPeerConnected;

        /// <summary>Called on the main thread when a client disconnects.</summary>
        public Action<TcpPeer>? OnPeerDisconnected;

        /// <summary>Called on the main thread when a complete message is received.</summary>
        /// <remarks>The byte array passed is ready to be loaded into a BitBuffer via buffer.FromSpan(new ReadOnlySpan&lt;byte&gt;(data, offset, length))</remarks>
        public Action<TcpPeer, byte[], int, int>? OnDataReceived;

        /// <summary>Returns the number of connected clients.</summary>
        public int ConnectedPeersCount => _peers.Count;

        /// <summary>Returns true if the server is actively listening.</summary>
        public bool IsRunning => _isRunning;

        public TcpServer()
        {
            _peers = new ConcurrentDictionary<uint, TcpPeer>();
            _eventQueue = new ConcurrentQueue<TcpEvent>();
        }

        /// <summary>
        /// Starts the server on the specified port.
        /// </summary>
        public void Start(int port)
        {
            if (_isRunning) throw new InvalidOperationException("Server is already running!");

            _listenerSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            _listenerSocket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
            _listenerSocket.NoDelay = true; // Disable Nagle for lower latency

            _listenerSocket.Bind(new IPEndPoint(IPAddress.Any, port));
            _listenerSocket.Listen(1000); // High backlog for MMORPG login spikes

            _isRunning = true;

            // Start accepting clients asynchronously
            StartAccept();
        }

        /// <summary>
        /// Stops the server and disconnects all clients.
        /// </summary>
        public void Stop()
        {
            if (!_isRunning) return;
            _isRunning = false;

            _listenerSocket?.Close();
            _listenerSocket = null;

            foreach (var peer in _peers.Values)
            {
                peer.Disconnect();
            }
            _peers.Clear();
        }

        /// <summary>
        /// MÁGICA: Call this method exactly once per frame (e.g., Unity's Update() method).
        /// This ensures all events (connections, data, disconnects) run on the Main Thread safely!
        /// </summary>
        public void Update()
        {
            // Process all queued events
            while (_eventQueue.TryDequeue(out TcpEvent ev))
            {
                switch (ev.Type)
                {
                    case TcpEventType.Connected:
                        OnPeerConnected?.Invoke(ev.Peer);
                        break;

                    case TcpEventType.Disconnected:
                        _peers.TryRemove(ev.Peer.PeerId, out _);
                        OnPeerDisconnected?.Invoke(ev.Peer);
                        break;

                    case TcpEventType.DataReceived:
                        OnDataReceived?.Invoke(ev.Peer, ev.Data!, ev.Offset, ev.Length);
                        if (ev.Data != null)
                        {
                            System.Buffers.ArrayPool<byte>.Shared.Return(ev.Data);
                        }
                        break;
                }
            }
        }

        /// <summary>
        /// Sends a message to all connected clients.
        /// </summary>
        public void Broadcast(BitBuffer buffer)
        {
            byte[] data = buffer.ToArray();
            foreach (var peer in _peers.Values)
            {
                peer.Send(data, 0, data.Length);
            }
        }

        /// <summary>
        /// Sends a message to a specific client.
        /// </summary>
        public void SendTo(uint peerId, BitBuffer buffer)
        {
            if (_peers.TryGetValue(peerId, out var peer))
            {
                peer.Send(buffer);
            }
        }

        // ════════════════════════════════════════════════════════
        // INTERNAL I/O (Runs on Background Threads)
        // ════════════════════════════════════════════════════════

        private void StartAccept()
        {
            if (!_isRunning || _listenerSocket == null) return;

            var acceptArgs = new SocketAsyncEventArgs();
            acceptArgs.Completed += AcceptCompleted;

            try
            {
                if (!_listenerSocket.AcceptAsync(acceptArgs))
                {
                    ProcessAccept(acceptArgs);
                }
            }
            catch (ObjectDisposedException) { }
        }

        private void AcceptCompleted(object? sender, SocketAsyncEventArgs e)
        {
            ProcessAccept(e);
        }

        private void ProcessAccept(SocketAsyncEventArgs e)
        {
            if (e.SocketError == SocketError.Success && e.AcceptSocket != null)
            {
                Socket clientSocket = e.AcceptSocket;
                clientSocket.NoDelay = true;

                uint peerId = _nextPeerId++;
                var peer = new TcpPeer(clientSocket, peerId, OnMessageReceivedFromPeer, OnPeerDisconnectedCallback);
                
                _peers.TryAdd(peerId, peer);

                // Queue Connected Event for the Main Thread
                _eventQueue.Enqueue(new TcpEvent { Type = TcpEventType.Connected, Peer = peer });

                // Start reading data
                peer.StartReceiving();
            }

            e.Dispose(); // Clean up current SAEA
            
            // Loop for next client
            StartAccept();
        }

        private void OnMessageReceivedFromPeer(TcpPeer peer, byte[] rawData, int offset, int length)
        {
            // Rent array from ArrayPool to prevent GC allocation
            byte[] msgData = System.Buffers.ArrayPool<byte>.Shared.Rent(length);
            Buffer.BlockCopy(rawData, offset, msgData, 0, length);

            _eventQueue.Enqueue(new TcpEvent
            {
                Type = TcpEventType.DataReceived,
                Peer = peer,
                Data = msgData,
                Offset = 0,
                Length = length
            });
        }

        private void OnPeerDisconnectedCallback(TcpPeer peer)
        {
            // Just queue it, Update() will remove it from Dictionary and call user event
            _eventQueue.Enqueue(new TcpEvent { Type = TcpEventType.Disconnected, Peer = peer });
        }

        public void Dispose()
        {
            Stop();
        }
    }
}

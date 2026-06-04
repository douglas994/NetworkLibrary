/*
 *  NetworkLibrary - High-Performance Networking for MMORPGs
 *  Module: Transport - TcpPeer
 *
 *  Represents a single active TCP connection.
 *  Uses SocketAsyncEventArgs for zero-allocation async I/O.
 *  Automatically handles length-prefix framing.
 */

using System;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using NetworkLibrary.Buffers;
using NetworkLibrary.Serialization;

namespace NetworkLibrary.Transport
{
    /// <summary>
    /// Represents a connected TCP endpoint (client on the server, or the server on the client).
    /// </summary>
    public sealed class TcpPeer : IDisposable
    {
        /// <summary>Unique ID assigned by the server. 0 if this is a client peer.</summary>
        public uint PeerId { get; internal set; }

        /// <summary>Remote IP and Port.</summary>
        public EndPoint? RemoteEndPoint { get; internal set; }

        /// <summary>Is the socket currently connected?</summary>
        public bool IsConnected => _socket != null && _socket.Connected && _isConnected;

        /// <summary>Ping / RTT in milliseconds (updated automatically if ping is enabled).</summary>
        public int Ping { get; internal set; }

        /// <summary>Attach any custom game data here (e.g., Player object).</summary>
        public object? UserData { get; set; }

        private Socket? _socket;
        private volatile bool _isConnected;
        
        // Zero-allocation async I/O
        private readonly SocketAsyncEventArgs _receiveArgs;
        private readonly byte[] _receiveBuffer;
        
        // Framing state
        private int _receiveOffset;
        private int _expectedLength;
        private bool _readingLength;

        // Callback to queue events to the main thread
        private readonly Action<TcpPeer, byte[], int, int>? _onMessageReceived;
        private readonly Action<TcpPeer>? _onDisconnected;

        internal TcpPeer(Socket socket, uint id, Action<TcpPeer, byte[], int, int> onMessageReceived, Action<TcpPeer> onDisconnected)
        {
            _socket = socket;
            PeerId = id;
            RemoteEndPoint = socket.RemoteEndPoint;
            _isConnected = true;

            _onMessageReceived = onMessageReceived;
            _onDisconnected = onDisconnected;

            // Pre-allocate buffer for async operations
            _receiveBuffer = new byte[65536]; // 64KB buffer
            _receiveArgs = new SocketAsyncEventArgs();
            _receiveArgs.SetBuffer(_receiveBuffer, 0, _receiveBuffer.Length);
            _receiveArgs.Completed += OnIoCompleted;

            _receiveOffset = 0;
            _expectedLength = 4; // Start by expecting 4 bytes for the length prefix
            _readingLength = true;
        }

        /// <summary>
        /// Starts listening for incoming data. Should only be called once after initialization.
        /// </summary>
        internal void StartReceiving()
        {
            if (!_isConnected || _socket == null) return;

            try
            {
                if (!_socket.ReceiveAsync(_receiveArgs))
                {
                    ProcessReceive(_receiveArgs);
                }
            }
            catch (ObjectDisposedException) { /* Ignored */ }
            catch (SocketException) { Disconnect(); }
        }

        /// <summary>
        /// Sends data over TCP.
        /// </summary>
        public void Send(byte[] data, int offset, int length)
        {
            if (!_isConnected || _socket == null) return;

            try
            {
                // Length-prefix framing (4 bytes little-endian)
                byte[] header = new byte[4];
                header[0] = (byte)(length & 0xFF);
                header[1] = (byte)((length >> 8) & 0xFF);
                header[2] = (byte)((length >> 16) & 0xFF);
                header[3] = (byte)((length >> 24) & 0xFF);

                // For absolute highest performance, we should use SAEA for sending too,
                // but Socket.Send is thread-safe and highly optimized in .NET for small/medium packets.
                _socket.Send(header, 0, 4, SocketFlags.None);
                _socket.Send(data, offset, length, SocketFlags.None);
            }
            catch (SocketException)
            {
                Disconnect();
            }
            catch (ObjectDisposedException) { }
        }

        /// <summary>
        /// Highly optimized integration: Sends a BitBuffer directly without heap allocation.
        /// </summary>
        public void Send(BitBuffer buffer)
        {
            int len = buffer.Length;
            byte[] rented = System.Buffers.ArrayPool<byte>.Shared.Rent(len);
            buffer.ToSpan(rented);
            Send(rented, 0, len);
            System.Buffers.ArrayPool<byte>.Shared.Return(rented);
        }

        /// <summary>
        /// Disconnects the peer gracefully.
        /// </summary>
        public void Disconnect()
        {
            if (!_isConnected) return;
            _isConnected = false;

            try { _socket?.Shutdown(SocketShutdown.Both); } catch { }
            try { _socket?.Close(); } catch { }

            _onDisconnected?.Invoke(this);
        }

        private void OnIoCompleted(object? sender, SocketAsyncEventArgs e)
        {
            if (e.LastOperation == SocketAsyncOperation.Receive)
            {
                ProcessReceive(e);
            }
        }

        private void ProcessReceive(SocketAsyncEventArgs e)
        {
            if (e.BytesTransferred > 0 && e.SocketError == SocketError.Success)
            {
                ProcessData(e.BytesTransferred);

                // Continue receiving
                try
                {
                    if (_socket != null && _isConnected)
                    {
                        if (!_socket.ReceiveAsync(e))
                        {
                            // Tail recursion if it completes synchronously
                            ProcessReceive(e);
                        }
                    }
                }
                catch { Disconnect(); }
            }
            else
            {
                // Disconnected
                Disconnect();
            }
        }

        private void ProcessData(int bytesTransferred)
        {
            _receiveOffset += bytesTransferred;
            int currentOffset = 0;

            while (_receiveOffset - currentOffset > 0)
            {
                if (_readingLength)
                {
                    if (_receiveOffset - currentOffset >= 4)
                    {
                        _expectedLength = _receiveBuffer[currentOffset] |
                                      (_receiveBuffer[currentOffset + 1] << 8) |
                                      (_receiveBuffer[currentOffset + 2] << 16) |
                                      (_receiveBuffer[currentOffset + 3] << 24);

                        if (_expectedLength <= 0 || _expectedLength > 10 * 1024 * 1024) // 10MB limit
                        {
                            Disconnect();
                            return;
                        }

                        _readingLength = false;
                        currentOffset += 4;
                    }
                    else
                    {
                        break; // Need more data for length
                    }
                }
                else
                {
                    if (_receiveOffset - currentOffset >= _expectedLength)
                    {
                        // Payload fully received
                        _onMessageReceived?.Invoke(this, _receiveBuffer, currentOffset, _expectedLength);
                        currentOffset += _expectedLength;
                        
                        // Reset for next message
                        _expectedLength = 4;
                        _readingLength = true;
                    }
                    else
                    {
                        break; // Need more data for payload
                    }
                }
            }

            // Move leftover data to the beginning of the buffer
            int leftover = _receiveOffset - currentOffset;
            if (leftover > 0 && currentOffset > 0)
            {
                Buffer.BlockCopy(_receiveBuffer, currentOffset, _receiveBuffer, 0, leftover);
            }
            _receiveOffset = leftover;

            // Set buffer for next receive (append mode)
            _receiveArgs.SetBuffer(_receiveOffset, _receiveBuffer.Length - _receiveOffset);
        }

        public void Dispose()
        {
            Disconnect();
            _receiveArgs.Dispose();
        }
    }
}

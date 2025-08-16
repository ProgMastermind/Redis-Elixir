#!/usr/bin/env python3
"""
Test script to verify PUBLISH command functionality
"""

import socket
import time

def send_command(sock, command):
    """Send a command and receive response"""
    sock.send(command.encode())
    response = sock.recv(1024)
    return response.decode()

def test_publish_functionality():
    """Test PUBLISH command with multiple subscribers"""
    print("=== Testing PUBLISH functionality ===")
    
    # Create multiple clients
    clients = []
    
    try:
        # Test 1: PUBLISH to a channel with no subscribers
        print("\n1. Testing PUBLISH to channel with no subscribers:")
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect(('localhost', 6379))
        clients.append(client)
        
        response = send_command(client, "*3\r\n$7\r\nPUBLISH\r\n$3\r\nfoo\r\n$3\r\nmsg\r\n")
        print(f"Response: {repr(response)}")
        print(f"Expected: ':0\\r\\n' (0 subscribers)")
        print(f"Match: {response == ':0\r\n'}")
        
        # Test 2: Subscribe one client to 'foo' channel
        print("\n2. Subscribing client 1 to 'foo' channel:")
        client1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client1.connect(('localhost', 6379))
        clients.append(client1)
        
        subscribe_response = send_command(client1, "*2\r\n$9\r\nSUBSCRIBE\r\n$3\r\nfoo\r\n")
        print(f"Subscribe response: {repr(subscribe_response)}")
        
        # Test 3: PUBLISH to 'foo' with 1 subscriber
        print("\n3. Testing PUBLISH to 'foo' with 1 subscriber:")
        response = send_command(client, "*3\r\n$7\r\nPUBLISH\r\n$3\r\nfoo\r\n$3\r\nmsg\r\n")
        print(f"Response: {repr(response)}")
        print(f"Response: {repr(response)}")
        print(f"Expected: ':1\\r\\n' (1 subscriber)")
        print(f"Match: {response == ':1\r\n'}")
        
        # Test 4: Subscribe two more clients to 'bar' channel
        print("\n4. Subscribing client 2 and 3 to 'bar' channel:")
        
        client2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client2.connect(('localhost', 6379))
        clients.append(client2)
        
        client3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client3.connect(('localhost', 6379))
        clients.append(client3)
        
        subscribe_response2 = send_command(client2, "*2\r\n$9\r\nSUBSCRIBE\r\n$3\r\nbar\r\n")
        print(f"Client 2 subscribe response: {repr(subscribe_response2)}")
        
        subscribe_response3 = send_command(client3, "*2\r\n$9\r\nSUBSCRIBE\r\n$3\r\nbar\r\n")
        print(f"Client 3 subscribe response: {repr(subscribe_response3)}")
        
        # Test 5: PUBLISH to 'bar' with 2 subscribers
        print("\n5. Testing PUBLISH to 'bar' with 2 subscribers:")
        response = send_command(client, "*3\r\n$7\r\nPUBLISH\r\n$3\r\nbar\r\n$3\r\nmsg\r\n")
        print(f"Response: {repr(response)}")
        print(f"Expected: ':2\\r\\n' (2 subscribers)")
        print(f"Match: {response == ':2\r\n'}")
        
        # Test 6: PUBLISH to 'foo' still has 1 subscriber
        print("\n6. Testing PUBLISH to 'foo' still has 1 subscriber:")
        response = send_command(client, "*3\r\n$7\r\nPUBLISH\r\n$3\r\nfoo\r\n$5\r\nhello\r\n")
        print(f"Response: {repr(response)}")
        print(f"Expected: ':1\\r\\n' (1 subscriber)")
        print(f"Match: {response == ':1\r\n'}")
        
        # Test 7: PUBLISH to non-existent channel
        print("\n7. Testing PUBLISH to non-existent channel:")
        response = send_command(client, "*3\r\n$7\r\nPUBLISH\r\n$7\r\nunknown\r\n$4\r\ntest\r\n")
        print(f"Response: {repr(response)}")
        print(f"Expected: ':0\\r\\n' (0 subscribers)")
        print(f"Match: {response == ':0\r\n'}")
        
    finally:
        # Clean up connections
        for client in clients:
            try:
                client.close()
            except:
                pass

if __name__ == "__main__":
    print("Make sure Redis server is running on localhost:6379")
    time.sleep(1)
    test_publish_functionality()

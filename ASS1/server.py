import json
import os
import socket
import bencodepy
import re
import pickle
import threading
import urllib.parse
import time
base_file_path = r"F:\HK5\MMT\BTL\test\server"

class peer_dict:
    def __init__(self,id,ip):
        self.id= id
        self.ip = ip
        self.port = ''
        self.file_piece = {}
        
    def info(self):
        print(f"ip:{self.ip},port:{self.port}: {self.file_piece}")
class  tracker:
    def __init__(self):
        self.order=0
        self.peers = {}
        self.clients = []
        #self.file_hash = {}
        self.file_name = []
        #self.piece_place = {}
        self.file_piece_hash = {}
        torrent_folder = self.getPath(base_file_path,"torrents")
        os.makedirs(torrent_folder, exist_ok=True)
    def removeinfo(self, peer):
        #print("Remove")
        target_peer = f'{peer.ip},{peer.port}'
        for key, peers in self.file_piece_hash.items():
            if target_peer in peers:
                peers.remove(target_peer)
        print(f"New file hash: {self.file_piece_hash}")
        to_remove = []
        for file_name in list(self.file_name):
                file = self.get_file_name(file_name)
                torrent_file_path = self.getPath(base_file_path,"torrents")
                torrent_file_path = self.getPath(torrent_file_path, f"{file}.torrent")
                with open(torrent_file_path, 'rb') as f:
                    torrent_data = f.read()
                decoded_torrent = bencodepy.decode(torrent_data)
                piece_count = decoded_torrent[b"info"][b"piece count"]

                all_pieces = set(range(1, piece_count + 1))
                pieces_have = set() 
                for key, peers in self.file_piece_hash.items():
                    if peers: 
                        _, piece_number = key.split(',')
                        if key.startswith(file_name):
                            pieces_have.add(int(piece_number))
                missing = all_pieces - pieces_have
                if missing:
                    print(f"Removing {file_name} because it is missing pieces: {missing}")
                    to_remove.append(file_name)
        for file_name in to_remove:
            self.file_name.remove(file_name)
            
        self.broadcast(f"File {to_remove} can not download due to a disconnected client")
        self.broadcast(f"File now can download: {self.file_name}")

    def broadcast(self, message):
        """Gửi tin nhắn đến tất cả các client"""
        for client in self.clients:
            try:
                client.sendall(message.encode())
            except Exception as e:
                print(f"Cannot sent message to client: {e}")
                self.clients.remove(client)
    def search_file_in_peers(self, file_name):
        found = False
        data = {}
        for peer_id, peer in self.peers.items():
            if file_name in peer.file_piece:
               # print(f"Peer ID: {peer_id}, IP: {peer.ip}, Port: {peer.port}")
                #message += f"Peer ID: {peer_id}, IP: {peer.ip}, Port: {peer.port}, Pieces:{peer.file_piece[file_name]}"
                if (f"{peer.ip},{peer.port}") not in data:
                    data[f"{peer.ip},{peer.port}"]=[]
                    
                data[f"{peer.ip},{peer.port}"].extend(peer.file_piece[file_name])
               # print(f"File '{file_name}' has pieces: {peer.file_piece[file_name]}")
                found = True
        if not found:
            print(f"No peer has the file '{file_name}'.")
        return data
    
    def handle_client(self,conn, addr):
        print(f"Connection established with {addr}")
        client_ip, client_port = addr
        peer = peer_dict(client_port,client_ip)
        self.order = self.order + 1
        this_order = self.order
        self.peers[self.order]= peer
        while True:
            try:
                message = conn.recv(1024)
                print(f"{addr} message: {message}")
                if message:
                    file_name =""
                    if message.startswith(b"DOWNLOAD: "):
                        message = message.decode()
                        file_name = message[len("DOWNLOAD: "):].strip() 
                        if file_name not in self.file_name: break 
                        data = self.search_file_in_peers(file_name)
                        mes = json.dumps(data)
                        conn.sendall(b"RESPOND DOWNLOAD: " + mes.encode())
                    elif message.startswith(b"TORRENT: "):
                        message = message.decode()
                        file_name = message[len("TORRENT: "):].strip()
                        if file_name not in self.file_name: 
                            conn.sendall(b"File does not exist")
                            continue
                        self.send_torrent(file_name,conn)
                    elif message.startswith(b"FILE: "):
                        message = message.decode()
                        file_piece = message[len("FILE: "):].strip()
                        for file_name, pieces in json.loads(file_piece).items():
                            self.peers[self.order].file_piece[file_name] = pieces
                            for piece in pieces:
                                if f"{file_name},{piece}" not in self.file_piece_hash:
                                    self.file_piece_hash[f"{file_name},{piece}"] = []                                
                                self.file_piece_hash[f"{file_name},{piece}"].append(f"{self.peers[this_order].ip},{self.peers[this_order].port}")
                        #print(self.file_piece_hash)
                        conn.sendall(b"OK")
                    elif message.startswith(b"PORT: "):
                        message = message.decode()
                        port = message[len("PORT: "):].strip()
                        self.peers[self.order].port = port
                        #print(self.peers[self.order].file_piece[file_name])
                        conn.sendall(b"OK")

                    elif message.startswith(b"UPDATE FILE: "):
                        message = message.decode()
                        update = message[len("UPDATE FILE: "):].strip()
                        file_name, piece = update.split(',')
                        if file_name not in self.peers[this_order].file_piece:
                            self.peers[this_order].file_piece[file_name] = []
                        self.peers[this_order].file_piece[file_name].append(piece)

                        if f"{file_name},{piece}" not in self.file_piece_hash:
                            self.file_piece_hash[f"{file_name},{piece}"] = []                                
                        self.file_piece_hash[f"{file_name},{piece}"].append(f"{self.peers[this_order].ip},{self.peers[this_order].port}")
                        #print(f"New file hash: {self.file_piece_hash}")
                        conn.sendall(b"OK")

                    elif message.startswith(b"SEND: "):
                        torrent_data = message[len(b"SEND: "):].strip()
                        decoded_torrent = bencodepy.decode(torrent_data)
                        #print(f'Nhận được : {decoded_torrent}')
                        file_name = decoded_torrent[b'info'][b'name'].decode('utf-8')
                        if file_name not in self.file_name:
                            self.file_name.append(file_name)
                            self.save_torrent(decoded_torrent,file_name)
                        self.broadcast(f"File can download: {self.file_name}")
                    elif message.startswith(b"PIECE: "):
                        message = message.decode()
                        piece = message[len("PIECE: "):].strip()
                        val = self.file_piece_hash.get(piece)
                        #val = [item for item in self.file_piece_hash.get(key, []) if item != (peer.ip, peer.port)]
                        response = f"{piece}: {val}"
                        #value = json.dumps(self.file_piece_hash.get(piece))
                        #print(self.peers[self.order].file_piece[file_name])
                        #print(response)
                        conn.sendall(f"RESPONSE PIECE: {response}".encode())
                    elif message.startswith(b"PEER"):
                        #message = message.decode()
                        #piece = message[len("PEER"):].strip()
                        list_peer = [f"{peer.ip},{peer.port}" for peer in self.peers.values()]
                        list_peer.remove(f"{peer.ip},{peer.port}")
                        #print(list_peer)
                        response = json.dumps(list_peer)
                        conn.sendall(f"RESPONSE PEER: {response}".encode())
                    elif message == b"INFO":
                        for key, peer in self.peers.items():
                            peer.info()
                        print(self.file_name)
                        print(self.file_piece_hash)
                        #print(self.clients)
                else:
                    break  
            except ConnectionError:
                print(f"Disconnect to client {addr}" )
                self.clients.remove(conn)
                self.removeinfo(peer)
                if this_order in self.peers:
                    del self.peers[this_order]  
                break
    

    def get_file_name(self,file_path):
        return os.path.splitext(os.path.basename(file_path))[0]
    

    #Torrent_path change
    def send_torrent(self,file_name,client_socket):
        file = self.get_file_name(file_name)
        #torrent_file_path = fr"F:\HK5\MMT\BTL\test\server\torrents\{file}.torrent"
        torrent_file_path = self.getPath(base_file_path, "torrents")
        torrent_file_path = self.getPath(torrent_file_path, f"{file}.torrent")
        with open(torrent_file_path, 'rb') as f:
            torrent_data = f.read()
            #decoded_torrent = bencodepy.decode(torrent_data)
            #print(f'Nhận được : {decoded_torrent}')
            client_socket.sendall(b"RESPOND TORRENT: " + torrent_data)

    #Torrent_path change
    def save_torrent(self,torrent,file_name):
        file = self.get_file_name(file_name)
        #torrent_file_path = fr"F:\HK5\MMT\BTL\test\server\torrents\{file}.torrent"
        torrent_file_path = self.getPath(base_file_path, "torrents")
        torrent_file_path = self.getPath(torrent_file_path, f"{file}.torrent")

        with open(torrent_file_path, 'wb') as f:
            f.write(bencodepy.encode(torrent))

    def peer_connect(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind(('0.0.0.0', 5000))  # Lắng nghe trên cổng 5000
        server_socket.listen()
        print("Server is waiting for a connection...")
        while True:
            client_socket, addr = server_socket.accept()
            # Tạo một luồng mới cho mỗi client
            self.clients.append(client_socket)
            client_thread = threading.Thread(target=self.handle_client, args=(client_socket, addr))
            client_thread.start()
        #   conn, addr = server_socket.accept()

    def getPath(self,base, file_name):
        return os.path.join(f"{base}", f"{file_name}")


server  = tracker()
server.peer_connect()
#server.send_file_name()
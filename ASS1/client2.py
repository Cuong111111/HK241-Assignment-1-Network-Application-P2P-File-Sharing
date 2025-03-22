import hashlib
import urllib.parse
import random
import time
import os
import socket
import pickle
import bencodepy
import json
import threading
from tqdm import tqdm
base_file_path = r"F:\HK5\MMT\BTL\test\client2"

class Client:
    def __init__(self):
        self.host = '127.0.0.1'
        self.port = 6883
        self.file_piece = {}
        self.file_piece_data = {}
        self.requested_piece = []  # Pieces that are requested
        self.missing_piece = []  # Pieces that are missing
        self.lock = threading.Lock()  # Threading lock
        self.connection_lock = threading.Lock()
        self.list_lock = threading.Lock() 
        self.connected = []  # List of connected peers
        self.client_socket = None 
        self.request_port = {}
        piece_folder = self.getPath(base_file_path,"pieces")
        os.makedirs(piece_folder, exist_ok=True)  # Tạo thư mục lưu các phần nếu chưa có
        torrent_folder = self.getPath(base_file_path,"torrents")
        os.makedirs(torrent_folder, exist_ok=True)
        self.merge_thread = None
        self.start_time=0
        self.start_time_up=0
        self.upload_peer = None
    # Setting folder path to locate pieces
    def get_pieces(self,file_path, piece_length):
        name = os.path.basename(file_path)
        filename = self.get_file_name(file_path)
        #piece_folder = f"F:/HK5/MMT/BTL/test/client1/pieces/{filename}"
        #piece_folder = self.getPath(base_file_path,"client1")
        piece_folder = self.getPath(base_file_path,"pieces")
        piece_folder = self.getPath(piece_folder,filename)
        if not os.path.exists(piece_folder):
            os.makedirs(piece_folder, exist_ok=True)  # Tạo thư mục lưu các phần nếu chưa có
        count = 1
        pieces = []

        with open(file_path, 'rb') as f:
            while True:
                piece = f.read(piece_length)
                if not piece:
                    break

                # Lưu phần vào một file riêng
                piece_filename = os.path.join(piece_folder, f"{count}")
                with open(piece_filename, 'wb') as piece_file:
                    piece_file.write(piece)

                # Lưu đường dẫn của phần vào file_piece_data
                if name not in self.file_piece_data:
                    self.file_piece_data[name] = []
                self.file_piece_data[name].append(piece_filename)

                if name not in self.file_piece:
                    self.file_piece[name] = []  # Khởi tạo danh sách các phần cho file mới

                 # Thêm piece vào danh sách
                self.file_piece[name].append(str(count))
                # Lưu thông tin hash của phần vào danh sách pieces
                pieces.append(hashlib.sha1(piece).digest())
                count += 1

        return pieces

    def create_torrent(self,file_path, piece_length, tracker_url):
        pieces = self.get_pieces(file_path, piece_length)
        num_pieces = len(pieces)
        torrent = {
            "announce": tracker_url,
            "info": {
                "name": os.path.basename(file_path),
                "piece length": piece_length,
                "pieces": b''.join(pieces),
                "file_length": os.path.getsize(file_path),
                "piece count": num_pieces
            }
        }
        self.save_torrent(torrent,file_path)
    

    def send_torrent(self,torrent_file_path,client_socket):
        with open(torrent_file_path, 'rb') as f:
            torrent_data = f.read()
            client_socket.sendall(b"SEND: " + torrent_data)
    def request_torrent(self,file_name,client_socket):
        message = f"TORRENT: {file_name}"
        client_socket.sendall((message).encode())
    def get_file_name(self,file_path):
        return os.path.splitext(os.path.basename(file_path))[0]
    

    #Torrent path save
    def save_torrent(self,torrent,file_name):
        file = self.get_file_name(file_name)
        # torrent_file_path = fr"F:\HK5\MMT\BTL\test\client1\torrents\{file}.torrent"
        #torrent_file_path = self.getPath(base_file_path,"client1")
        torrent_file_path = self.getPath(base_file_path,"torrents")
        if not os.path.exists(torrent_file_path):
            os.makedirs(torrent_file_path,exist_ok=True)       
        torrent_file_path = self.getPath(torrent_file_path, f"{file}.torrent")

        with open(torrent_file_path, 'wb') as f:
            f.write(bencodepy.encode(torrent))
    
    def merge_pieces(self, output_folder, piece_folder, file_name, all_pieces):
            pieces_have = self.file_piece.get(f'{file_name}', [])  
            missing = set(all_pieces) - set(pieces_have)
            if missing:
                #print("Chưa đủ")
                return
            file = self.get_file_name(file_name)
            pieces = sorted(map(int, self.file_piece.get(file_name, [])))
            os.makedirs(output_folder, exist_ok=True)
            output_file_path = os.path.join(base_file_path, f"{file_name}")
            with open(output_file_path, "wb") as output_file:
                for piece in pieces:
                    piece_path = os.path.join(piece_folder,"pieces",file, str(piece))
                    with open(piece_path, "rb") as piece_file:
                        output_file.write(piece_file.read())
            end_time = time.time()
            #print(f"Start {self.start_time}")
            #print(f"End {end_time}")
            elapsed_time = end_time - self.start_time        
            print(f"Complete download file in {elapsed_time}s")
    def printinfo(self):
        print(self.file_piece)



    def ConnectServer(self):
        threading.Thread(target=self.request_handler, daemon=True).start()
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client_socket.connect(('127.0.0.1', 5000))

        # Chuyển dictionary thành chuỗi JSON
        message = json.dumps(self.file_piece)
        # Gửi chuỗi JSON đã mã hóa qua socket
        self.client_socket.sendall(f"PORT: {self.port}".encode())
        self.client_socket.recv(8)
        self.client_socket.sendall(f"FILE: {message}".encode())
        self.client_socket.recv(8)

        for file_name in os.listdir(base_file_path):
            file_path = os.path.join(base_file_path, file_name)
            if os.path.isfile(file_path):
                name = self.get_file_name(file_path)
                torrent_folder_path = self.getPath(base_file_path, "torrents")
                torrent_file_path = os.path.join(torrent_folder_path, f'{name}.torrent')
                if os.path.isfile(torrent_file_path):
                    self.send_torrent(torrent_file_path, self.client_socket)
  
    

        receive_thread = threading.Thread(target=self.receive_messages,args=(self.client_socket,))
        send_thread = threading.Thread(target=self.send_messages, args=(self.client_socket,))
        receive_thread.start()
        send_thread.start()

    def send_messages(self,client_socket):
         while True:
            message = input()
              # Đợi người dùng nhập tin nhắn
            #message = "DOWNLOAD: file2.txt"
            if message.strip().lower() == 'exit':
                print("Disconnect to server.")
                client_socket.close()
                break
            elif message.startswith("DOWNLOAD: "):
                file_name = message[len("DOWNLOAD: "):].strip()
                if file_name not in self.file_piece:
                    self.request_torrent(file_name,client_socket)
                else:
                    torrent_data = ""
                    file = self.get_file_name(file_name)
                    torrent_file_path = self.getPath(base_file_path,"torrents")
                    torrent_file_path = self.getPath(torrent_file_path, f"{file}.torrent")
                    
                    if not os.path.exists(torrent_file_path):
                        os.makedirs(torrent_file_path, exist_ok=True)
                    with open(torrent_file_path, 'rb') as f:
                        torrent_data = f.read()
                    decoded_torrent = bencodepy.decode(torrent_data)
                    piece_count = decoded_torrent[b"info"][b"piece count"]
                    # Tạo danh sách các piece từ 1 đến piece_count
                    all_pieces = [str(i) for i in range(1, piece_count + 1)]
                    pieces_have = self.file_piece.get(f'{file_name}', [])  
                    #print(pieces_have)
                    missing = set(all_pieces) - set(pieces_have)
                    self.missing_piece = [f"{file_name},{i}" for i in missing]
                    #print("Missing pieces:", self.missing_piece)
                    missing_count = len(missing)

                    #for i in range(1, missing_count + 1):
                    self.start_time= time.time()
                    threading.Thread(target=self.send_request,args=(all_pieces,)).start()
                    threading.Thread(target=self.send_request,args=(all_pieces,)).start()
                    #threading.Thread(target=self.send_request,args=(all_pieces,)).start()


                    
            
            elif message == 'info':
                print(self.file_piece)
                client_socket.sendall(b"INFO")
            elif message.startswith("UPLOAD: "):
                file_name = message[len("UPLOAD: "):].strip()
                if file_name not in self.file_piece:
                    print("Warning: NOT HAVE FILE!")
                    continue
                self.client_socket.sendall("PEER".encode())
                
            else:
                print("Warning: WRONG FORMAT INPUT")

    def receive_messages(self,client_socket):
        while True:
            try:
                response = client_socket.recv(1024)
                if response.startswith(b"File "):
                    response = response.decode()
                    print(f"Server: {response}")
                elif response.startswith(b"RESPOND TORRENT: "):
                    torrent_data = response.split(b'RESPOND TORRENT: ')[1]
                    decoded_torrent = bencodepy.decode(torrent_data)
                   # print(f'Nhận được torrent: {decoded_torrent}')
                    file_name = decoded_torrent[b'info'][b'name'].decode('utf-8')
                    if file_name not in self.file_piece:
                        self.file_piece[file_name] = []  
                    self.save_torrent(decoded_torrent,file_name)
                    piece_count = decoded_torrent[b"info"][b"piece count"]
   
                    all_pieces = [str(i) for i in range(1, piece_count + 1)]
                    pieces_have = self.file_piece.get(f'{file_name}', [])  
                    #print(pieces_have)
                    missing = set(all_pieces) - set(pieces_have)
                    self.missing_piece = [f"{file_name},{i}" for i in missing]
                    #print("Missing pieces:", self.missing_piece)
                    # missing_count = len(missing)
                    # threads = []
                    # for i in range(1, missing_count + 1):
                    #     t = threading.Thread(target=self.send_request,args=(all_pieces,))
                    #     t.start()
                    #     threads.append(t)
                    self.start_time= time.time()
                    #print(f"Start {self.start_time}")
                    threading.Thread(target=self.send_request,args=(all_pieces,)).start()
                    threading.Thread(target=self.send_request,args=(all_pieces,)).start()
                    #threading.Thread(target=self.send_request,args=(all_pieces,)).start()
                elif response.startswith(b"RESPOND DOWNLOAD: "):
                    blist = response.split(b'RESPOND DOWNLOAD: ')[1]
                    list = json.loads(blist.decode())
                    #print(f"Server: {list}")
                elif response.startswith(b"RESPONSE PIECE: "):
                    blist = response.split(b'RESPONSE PIECE: ')[1]
                    list = blist.decode()
                    parts = list.split(': ', 1)
                    left_part = parts[0].strip()
                    right_part = parts[1].strip()
                    right_list = eval(right_part)
                    
                    if left_part not in self.request_port:
                        self.request_port[left_part] = []
                    for entry in right_list:
                        ip, port = entry.split(',')
                        self.request_port[left_part].append((ip, int(port)))
                    #print(f"Server: RESPONSE PIECE: {self.request_port}")
                elif response.startswith(b"RESPONSE PEER: "):
                    blist = response.split(b'RESPONSE PEER: ')[1]
                    list = json.loads(blist.decode())
                    #print(f"Server: RESPONSE PEER: {list}")
                    list = json.loads(blist.decode())
                    peer_list = []
                    for entry in list:
                        ip, port = entry.split(',')
                        peer_list.append((ip, int(port)))
                    #print(f"Server: {peer_list}")
                    self.upload_peer = peer_list
                    self.start_time_up = time.time()
                    
                    for i,peer in enumerate(peer_list):
                            start_time = time.time()
                            piece = [(file_name, piece) for file_name, pieces in self.file_piece.items() for piece in pieces]
                            #num_piece = min(3, len(piece))  # Ensure num_piece does not exceed available pieces
                            num_piece = min(int(len(piece)/5),3)
                            if(num_piece == 0): 
                                num_piece = 1
                            #pieces = random.sample(self.piece, num_piece)
                            pieces = [piece[(i+j)%len(piece)] for j in range(num_piece)]
                            #print(f"{num_piece} and {pieces}")
                            # Create a connection
                            upload_thread = threading.Thread(target=self.upload, args=(peer,pieces,start_time))
                            upload_thread.start()
                        
            except ConnectionError:
                print("Disconnect to server.")
                break

 
    def request_handler(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((self.host, self.port))
        server_socket.listen(5)
        print(f"Listening on port {self.port}")
        while True:
            client_socket, addr = server_socket.accept()
            self.send_handler(client_socket,addr)

    def send_handler(self, client_socket,addr):
        print(f"Handle Request from peer: {addr}")
        try:
            with client_socket:
                initial_message = client_socket.recv(1024).decode()
                if not initial_message:
                    print("No initial message received.")
                    return
                elif initial_message == "down":
                    client_socket.sendall(b"OK")
                    file_name = client_socket.recv(1024).decode()
                    list_piece = self.file_piece[file_name]
                    piece = [f"{file_name},{piece}" for piece in list_piece]
                    client_socket.sendall(pickle.dumps(piece))
                    while True:
                        start = client_socket.recv(5).decode()
                        if start != "start":
                            break
                        message = client_socket.recv(1024).decode()
                        if not message:
                            print("No message received.")
                            continue
                        file_name, piece_name = message.split(',')
                        file_name = self.get_file_name(file_name)
                        piece_path = os.path.join(base_file_path,"pieces", file_name, piece_name)
                        #piece_path = self.getPath(base_file_path, "client2")
                        #piece_path = self.getPath(base_file_path, "pieces")
                        #piece_path = self.getPath(base_file_path, file_name)
                        #piece_path = self.getPath(piece_path, piece_name)
                        #print(piece_path)
                        if os.path.exists(piece_path):
                            file_size = os.path.getsize(piece_path)
                            client_socket.sendall(str(file_size).encode())
                            ack = client_socket.recv(1024).decode()
                            
                            if ack == "READY":
                                with open(piece_path, 'rb') as f:
                                    progress = tqdm(total=file_size, unit='B', unit_scale=True, desc=f"Uploading file: {file_name}, piece: {piece_name}  to {addr}")
                                    while True:
                                        bytes_read = f.read(4096)
                                        if not bytes_read:
                                            break
                                        client_socket.sendall(bytes_read)
                                        progress.update(len(bytes_read))
                                    progress.close()

                        else:
                            client_socket.sendall(b"NOTFOUND")
                    print(f"Finish handle request from peer: {addr}")
                        
                elif initial_message == "upup" :
                    while True: 
                        client_socket.sendall(b"Start")
                        message = client_socket.recv(1024)
                        if message.startswith(b"SEND: "):
                            torrent_data = message[len(b"SEND: "):].strip()
                            decoded_torrent = bencodepy.decode(torrent_data)
                            #print(f'Receive torrent file')
                            file_name = decoded_torrent[b'info'][b'name'].decode('utf-8')
                            self.save_torrent(decoded_torrent,file_name)
                        client_socket.sendall(b'OK')
                        message = client_socket.recv(1024).decode()
                        file_name, piece_name = message.split(',')
                        print(f"Start receive {message} from {addr}")
                        file_name_1 = self.get_file_name(file_name)
                        file_path = os.path.join(base_file_path,"pieces", file_name_1)
                        if not os.path.exists(file_path):
                            os.makedirs(file_path, exist_ok=True)

                        client_socket.sendall(b"Start")
                            #file_size_data = client_socket.recv(1024).decode().strip()
                        file_size = int(client_socket.recv(1024).decode())
                        #print(f"file_size: {file_size}")
                        progress = tqdm(total=file_size, unit='B', unit_scale=True, desc=f"Receiving file: {file_name}, piece: {piece_name} from {addr}: ")
                        client_socket.sendall(b"READY") 
                        received_size = 0
                        piece_data = b''
                        while received_size < file_size:
                            data = client_socket.recv(4096)
                            #print(f"data: {data}") 
                            if not data:
                                break
                            piece_data += data
                            received_size += len(data)
                            progress.update(len(data))
                        #file_path = os.path.join(self.folder, str(file_name))
                        #os.makedirs(file_path, exist_ok=True)
                        piece_path = os.path.join(base_file_path,"pieces", file_name_1, piece_name)
                        with open(piece_path, 'wb') as f:
                            f.write(piece_data)
                        client_socket.sendall(b"Finish")
                        progress.close()
                        print(f"Finish receive {message} from {addr}")
                        with self.lock:
                            #self.missing_piece.remove((file_name, piece_name))
                            #self.requested_piece.remove((file_name, piece_name))
                            #self.piece.append((file_name, piece_name))
                            if file_name not in self.file_piece:
                                self.file_piece[file_name] = []  
                            self.file_piece[file_name].append(piece_name) 
                            self.client_socket.sendall(f'UPDATE FILE: {file_name},{piece_name}'.encode())
                        initial_message = client_socket.recv(4).decode()
                        if(initial_message != "upup"): 
                            break                
        except Exception as e:
            print(f"Error handling send request in : {e}")

    def send_request(self,all_pieces):
        #print("SEND REQUEST")
        while True:
            if not self.missing_piece:
                break
            # try:
            with self.lock:
                piece = None
                pos = None
                for i, x in enumerate(self.missing_piece):
                    if x in self.requested_piece:
                        continue
                    piece = x # piece = 'Filename,piece_index'
                    pos = i
                    self.missing_piece.remove(x)
                    self.client_socket.sendall(f"PIECE: {piece}".encode())
                    break

            if piece is not None:
                #print(f"PIECE: {piece}")
                
                #print("Receive")
                count = 0
                while piece not in self.request_port: 
                    time.sleep(0.01)
                    count += 1
                    if(count == 1000): 
                        print("Time Error!!")
                        return
                list_port = self.request_port[piece]
                #print(list_port)
                #target_port = 5001 if self.port == 5000 else 5000
                #list_port = [('127.0.0.1', target_port)]
                with self.connection_lock:
                    for connected_port in self.connected:
                        if connected_port in list_port:
                            list_port.remove(connected_port)
                    
                    if not list_port:
                        #print("No available peer to connect.")
                        self.missing_piece.append(piece)
                        time.sleep(1)
                        continue

                    target = random.choice(list_port)
                    self.connected.append(target)
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect(target)
                with s:
                    print(f"Start download from peer:{target}")
                    
                    s.sendall(b"down")
                    s.recv(8)
                    file_name, piece_name = piece.split(',')
                    s.sendall(file_name.encode())
                    list_data = s.recv(1024)
                    available_pieces = pickle.loads(list_data)
                   # print(available_pieces)
                    #print (self.missing_piece)
                    requested = [x for x in self.missing_piece if x in available_pieces]
                    #print(piece)
                    requested.append(piece)
                    #print(requested)
                    
                    for request in requested:
                        file_name, piece_name = request.split(',')
                        with self.list_lock:
                            
                            if (file_name, piece_name) not in self.requested_piece:
                                self.requested_piece.append((file_name, piece_name))
                               # print(request)
                               # print(self.requested_piece)
                                #print(f"Request {request} to {target}")
                            else:
                                continue
                            
                            
                        
                        s.sendall(b"start")
                        
                        message = f"{file_name},{piece_name}".encode()
                        s.sendall(message)

                        file_size_data = s.recv(1024).decode().strip()
                        if not file_size_data:
                            print(f" No data received for file size.")
                            continue
                        try:
                            file_size = int(file_size_data)
                        except ValueError:
                            print(f" Invalid data for file size: '{file_size_data}'")
                            continue
                        progress = tqdm(total=file_size, unit='B', unit_scale=True, desc=f"Downloading file: {file_name}, piece: {piece_name} from {target}")
                        start_time = time.time()
                        s.sendall(b"READY")
                        received_size = 0
                        piece_data = b''
                        while received_size < file_size:
                            data = s.recv(4096)
                            if not data:
                                break
                            piece_data += data
                            received_size += len(data)
                            end_time = time.time()
                            elapsed_time = end_time - start_time
                            progress.update(len(data))
                            progress.set_description(f"Downloading file: {file_name}, piece: {piece_name} from {target} | Time: {elapsed_time}s")
                        file_names = self.get_file_name(file_name)
                        #file_path = os.path.join(r"F:\HK5\MMT\BTL\test\client1\pieces", str(file_names))
                        #file_path = self.getPath(base_file_path, "client1")
                        file_path = self.getPath(base_file_path, "pieces")
                        file_path = self.getPath(file_path, file_names)
                        if not os.path.exists(file_path):
                            os.makedirs(file_path, exist_ok=True)
                        piece_path = os.path.join(file_path, str(piece_name))
                        with open(piece_path, 'wb') as f:
                            f.write(piece_data)
                        progress.close()
                        #print(f"Download Complete: Receive {request} from {target}")
                        with self.lock:
                            if f'{file_name},{piece_name}' in self.missing_piece:
                                self.missing_piece.remove(f'{file_name},{piece_name}')
                            
                            #
                            
                            if file_name not in self.file_piece:
                                self.file_piece[file_name] = []  
                            self.file_piece[file_name].append(piece_name)
                            self.client_socket.sendall(f'UPDATE FILE: {file_name},{piece_name}'.encode())
                            #print(self.missing_piece)
                            self.merge_pieces(base_file_path, base_file_path, file_name, all_pieces)
                            #self.piece.append((file_name, piece_name))
                        #print(f"{self.folder} received {piece_name} from peer on port {target_port}")
                    s.sendall(b"Enddd")
                    self.connected.remove(target)
            
        
            return

    def getPath(self,base, file_name):
        return os.path.join(f"{base}", f"{file_name}")

    def upload(self,peer,pieces,start_time):
          # Full piece transfer using one connection
        #target_port = 5000
        #list_peer = [('127.0.0.1', target_port)]
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(peer)
        print(f"Start upload to peer:{peer}")
        for piece in pieces:
            try:
                file_name, piece_name = piece
                file_name = str(file_name)
                piece_name = str(piece_name)
                # Send initial message to start upload
                s.sendall(b"upup")
                s.recv(8)
                file = self.get_file_name(file_name)
                torrent_file_path = self.getPath(base_file_path,"torrents")  
                torrent_file_path = self.getPath(torrent_file_path, f"{file}.torrent")
                self.send_torrent(torrent_file_path,s)
                s.recv(8)
                message = f"{file_name},{piece_name}".encode()
                file = self.get_file_name(file_name)
                piece_path = os.path.join(base_file_path,"pieces", file, piece_name)
                #print (piece_path)
                s.sendall(message)
                s.recv(8)
                if os.path.exists(piece_path):
                    file_size = os.path.getsize(piece_path)
                    s.sendall(str(file_size).encode())
                    ack = s.recv(1024).decode()
                    if ack == "READY":
                        with open(piece_path, 'rb') as f:
                            progress = tqdm(total=file_size, unit='B', unit_scale=True, desc=f"Uploading file: {file_name}, piece: {piece_name}  to {peer}")
                            #start_time = time.time()
                            #print(f"Start {start_time}")
                            while True:
                                bytes_read = f.read(4096)
                                if not bytes_read:
                                    break
                                s.sendall(bytes_read)
                                progress.update (len(bytes_read))
                                end_time = time.time()
                                elapsed_time = end_time - start_time
                                progress.set_description(f"Uploading file: {file_name}, piece: {piece_name} to {peer} | Time: {elapsed_time:.2f}s")
                            progress.close()
                        
                        final = s.recv(6).decode()
                        if (final!= "Finish"): 
                            print("Wrong")
                            break
                else:
                    s.sendall(b"NOTFOUND")
                
            except (ConnectionResetError, BrokenPipeError) as e:
                print(f"Connection error during upload of {piece_name}: {e}")
                s.close()
                print("Reconnecting...")
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect(peer)
                continue  # Continue to the next piece
        s.sendall(b"Done")
        s.close()
        self.upload_peer.remove(peer)
        if not self.upload_peer:
            time.sleep(0.0001)
            end_time_up = time.time()
            elapsed_time_up = end_time_up - self.start_time_up
            print(f"Complete upload in {elapsed_time_up:.2f}s", flush=True)


            


client1 = Client()


for file_name in os.listdir(base_file_path):
    file_path = os.path.join(base_file_path, file_name)
    if os.path.isfile(file_path):  # Chỉ lấy file
        torrent = client1.create_torrent(file_path, 512 * 1024, "http://tracker-url.com/announce")
client1.ConnectServer()
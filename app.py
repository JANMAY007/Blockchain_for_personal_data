# Imported required libraries
import os
import hashlib
import json
import datetime
from urllib.parse import urlparse
from uuid import uuid4
import requests
from flask import Flask, jsonify, request, render_template, redirect
from argparse import ArgumentParser
from werkzeug.utils import secure_filename
import pandas as pd


# Blockchain class
class Blockchain:
    def __init__(self):  # constructor
        self.current_transactions = []  # details of the block
        self.chain = []  # chain of the block
        self.nodes = set()  # node of the block
        self.new_block(previous_hash='0', proof=100)  # Create the genesis block

    def register_node(self, address):  # function to register the new node
        # parse the url into required address
        parsed_url = urlparse(address)
        if parsed_url.netloc:
            self.nodes.add(parsed_url.netloc)
        elif parsed_url.path:
            self.nodes.add(parsed_url.path)
        else:
            raise ValueError('Invalid URL')

    def valid_chain(self, chain):  # function to check the validity of the chain
        last_block = chain[0]
        current_index = 1
        while current_index < len(chain):  # checking the whole chain
            block = chain[current_index]
            # displaying the blocks
            print(f'{last_block}')
            print(f'{block}')
            print("\n-----------\n")
            # Check that the hash of the block is correct
            last_block_hash = self.hash(last_block)
            # checking that if consecutive blocks are related or not
            if block['previous_hash'] != last_block_hash:
                return False
            # Check that the Proof of Work is correct
            if not self.valid_proof(last_block['proof'], block['proof'], last_block_hash):
                return False
            last_block = block
            current_index += 1
        return True

    def resolve_conflicts(self):
        """
        Resolving the conflict if chain is needed to be replaced or not
        :return: True if replaced
        """
        neighbours = self.nodes
        new_chain = None
        # We're only looking for chains longer than ours
        max_length = len(self.chain)
        # Grab and verify the chains from all the nodes in our network
        for node in neighbours:
            response = requests.get(f'https://{node}/chain')
            if response.status_code == 200:
                length = response.json()['length']
                chain = response.json()['chain']
                # Check if the length is longer and the chain is valid
                if length > max_length and self.valid_chain(chain):
                    max_length = length
                    new_chain = chain
        # Replace our chain if we discovered a new, valid chain longer than ours
        if new_chain:
            self.chain = new_chain
            return True
        return False

    def new_block(self, proof, previous_hash):  # Create a new Block in the Blockchain
        """
        :param proof: The proof given by the Proof of Work algorithm
        :param previous_hash: Hash of previous Block
        :return: New Block
        """
        block = {
            'index': len(self.chain) + 1,
            'timestamp': str(datetime.datetime.now()),
            'transactions': self.current_transactions,
            'proof': proof,
            'previous_hash': previous_hash or self.hash(self.chain[-1]),
        }
        # Reset the current list of transactions
        self.current_transactions = []
        self.chain.append(block)
        return block

    def new_transaction(self, sender, recipient, amount):
        """
        Creates a new transaction to go into the next mined Block

        :param sender: Address of the Sender
        :param recipient: Address of the Recipient
        :param amount: Amount
        :return: The index of the Block that will hold this transaction
        """
        self.current_transactions.append({
            'sender': sender,
            'recipient': recipient,
            'amount': amount,
        })
        return self.last_block['index'] + 1

    @property
    def last_block(self):  # function to return the last block
        return self.chain[-1]

    @staticmethod
    def hash(block: object) -> object:  # applying SHA-256 to our block data
        # We must make sure that the Dictionary is Ordered, or we'll have inconsistent hashes
        block_string = json.dumps(block, sort_keys=True).encode()
        return hashlib.sha256(block_string).hexdigest()

    def proof_of_work(self, last_block):
        """
        validating the blockchain by consecutive blocks
        with the help of proof of last block
        :param last_block: provides proof
        :return: proof of current block
        """
        last_proof = last_block['proof']
        last_hash = self.hash(last_block)
        proof = 0
        while self.valid_proof(last_proof, proof, last_hash) is False:
            proof += 1
        return proof

    @staticmethod
    def valid_proof(last_proof, proof, last_hash):  # create full block hash key with last block hash.
        """
        Validates the Proof
        :param last_proof: <int> Previous Proof
        :param proof: <int> Current Proof
        :param last_hash: <str> The hash of the Previous Block
        :return: <bool> True if correct, False if not.
        """
        guess = f'{last_proof}{proof}{last_hash}'.encode()
        guess_hash = hashlib.sha256(guess).hexdigest()
        return guess_hash[:4] == "0000"


"""
Building the project
before running the flask app
"""
# Instantiate the Node
app = Flask(__name__)
UPLOAD_FOLDER = 'static/files/'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
# Generate a globally unique address for this node
node_identifier = str(uuid4()).replace('-', '')
# Instantiate the Blockchain
blockchain = Blockchain()
# taking the data from dataset

file_name = ''
column_names = []
true_cols = []  # respective column apart from hidden data.


@app.route('/blockchain.html', methods=['GET'])
def mine():  # function to mine the data onto blocks
    # We run the proof of work algorithm to get the next proof
    last_block = blockchain.last_block
    proof = blockchain.proof_of_work(last_block)  # it will get pow of last block.
    # The sender is "0" to signify that this node has mined a new block of data.
    blockchain.new_transaction(sender="0", recipient=node_identifier, amount=1)
    # Forge the new Block by adding it to the chain
    previous_hash = blockchain.hash(last_block)
    block = blockchain.new_block(proof, previous_hash)
    data_fields = []
    data = pd.read_csv('static/files/' + file_name)
    data = pd.DataFrame(data)
    """
    In this it will check the datatype 
    (str,int,float) for all the rows of 
    respective column.
    It will store the hash of the respective
    data in data_fields list.
     """
    for cols in range(len(column_names)):
        if data.dtypes[cols] == 'object':
            data_fields.append(blockchain.hash(str(data[column_names[cols]].iloc[block['index'] - 2])))
        elif data.dtypes[cols] == 'int64':
            data_fields.append(blockchain.hash(int(data[column_names[cols]].iloc[block['index'] - 2])))
        elif data.dtypes[cols] == 'float64':
            data_fields.append(blockchain.hash(float(data[column_names[cols]].iloc[block['index'] - 2])))
    print("data fields")
    print(data_fields)
    """
    In this it will check the datatype 
    (str,int,float) for all the rows of 
    respective hidden column.
    It will mask the data
    """
    hidden = []
    # it will check datatype for type casting
    for cols in range(len(true_cols)):
        if data.dtypes[cols] == 'object':
            hidden.append('*' * len(str(data[true_cols[cols]].iloc[block['index'] - 2])))
        elif data.dtypes[cols] == 'int64':
            hidden.append('*' * len(str(int(data[true_cols[cols]].iloc[block['index'] - 2]))))
        elif data.dtypes[cols] == 'float64':
            hidden.append('*' * len(str(float(data[true_cols[cols]].iloc[block['index'] - 2]))))
    print("hidden")
    print(hidden)
    """
     In this it will check the respective column 
     (total cols- hidden cols)
    """
    display_columns = [temp for temp in column_names if temp not in true_cols]
    display = []
    for cols in range(len(display_columns)):
        display.append(data[display_columns[cols]].iloc[block['index'] - 2])
    print("display")
    print(display)

    # data to display in block on UI
    response = {
        'index': block['index'],
        'transactions': block['transactions'],
        'proof': block['proof'],
        'previous_hash': block['previous_hash'],
    }
    # hashed_data stores all the hash key of data of all the columns of respective blocks (row)
    hashed_data = {data_fields[i]: data_fields[i] for i in range(0, len(data_fields))}
    # hidden_data stores all the masked columns.
    hidden_data = {hidden[i]: hidden[i] for i in range(0, len(hidden))}
    # display_data stores all the columns except the masked one.
    display_data = {display[i]: display[i] for i in range(0, len(display))}
    return render_template('blockchain.html', response=response, hashed_data=data_fields, hidden_data=hidden,
                           display_data=display, hidden_len=len(hidden_data), display_len=len(display_data),
                           hashed_len=len(hashed_data), true_cols=true_cols,
                           display=[x for x in column_names if x not in true_cols])


@app.route('/transactions/new', methods=['POST'])
def new_transaction():  # function to initialise new transaction of data for our blockchain
    values = request.get_json()
    # Check that the required fields are in the method POST of form w.r.t data
    required = ['sender', 'recipient', 'amount']
    if not all(k in values for k in required):
        return 'Missing values', 400
    # Create a new Transaction
    index = blockchain.new_transaction(values['sender'], values['recipient'], values['amount'])
    response = {'message': f'Transaction will be added to Block {index}'}
    return jsonify(response), 201


@app.route('/all_blocks.html', methods=['GET'])
def full_chain():  # function to display full blockchain in UI
    temp = blockchain.chain[:]
    return render_template('all_blocks.html', response=temp[1:])


@app.route('/', methods=['GET'])
def home():
    return render_template('base.html')


@app.route('/nodes/register', methods=['POST'])
def register_nodes():  # Function to register node of chain
    values = request.get_json()  # taking value for node
    nodes = values.get('nodes')
    if nodes is None:
        return "Error: Please supply a valid list of nodes", 400
    for node in nodes:
        blockchain.register_node(node)
    # providing appropriate response
    response = {
        'message': 'New nodes have been added',
        'total_nodes': list(blockchain.nodes),
    }
    return jsonify(response), 201


@app.route('/nodes/resolve', methods=['GET'])
def consensus():  # function to verify in proof of work that the chain has been replaced or not
    replaced = blockchain.resolve_conflicts()  # conflicts function will call here
    if replaced:
        response = {
            'message': 'Our chain was replaced',
            'new_chain': blockchain.chain
        }
    else:
        response = {
            'message': 'Our chain is authoritative',
            'chain': blockchain.chain
        }
    return jsonify(response), 200


ALLOWED_EXTENSIONS = {'csv'}

def allowed_file(filename):
    # it will check the all extension files.
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route("/upload_file.html", methods=['GET', 'POST'])
def upload_files():
    global column_names, true_cols, file_name
    """
    it will check the method of form is post or not
    if the method is post then it will check the two 
    individual forms named as 'form1 and form 2'
    """
    if request.method == 'POST':
        form_name = request.form['form-name']
        if form_name == 'form1':
            file = request.files['file']
            if file.filename == '':
                print('No file selected')
                return redirect(request.url)
            if file:
                filename = secure_filename(file.filename)
                file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
                file_name = str(file.filename)
                column_names = list(pd.read_csv('static/files/' + str(file.filename)).columns)
                print('all column of uploaded files')
                print(column_names)
                return render_template('columns.html', column_names=column_names)
        elif form_name == 'form2':
            true_cols = request.form.getlist('mycheckbox')
            print('it will check all selected columns')
            print(true_cols)  # it will get all the selected checkbox.
            return render_template('base.html')
    return render_template('upload_file.html')


if __name__ == '__main__':  # main statement
    parser = ArgumentParser()  # initialising object or ArgumentParser
    # parsing the arguments into executable form
    parser.add_argument('-p', '--port', default=5000, type=int, help='port to listen on')
    args = parser.parse_args()
    port = args.port
    # running the built flask app
    app.debug = True
    app.run(host='0.0.0.0', port=port)

## Requirements
Third-party libraries used:
    1. PySpark
    2. Numpy


## Usage
In order to use our simple search engine, you need to first put all of the documents under the "corpus" folder. 
This is the collection of documents which is being searched. 

Before searching, you need to build an inverted index for your corpus. To do this, you need to run:
    $ python dataPreprocessing.py

Then you can start to search your query! There are two command line arguments for the search.py. 
The first command line argument is your query, and this is required. The corresponding flags are '-q' or '--query'
The second command line argument is the number of most relevant documents retrieved by the model. This is optional and the default value is 5. The corresponding flags are '-k' or '--topk'
To search for a query, you need to run:
    $ python search.py -q YOUR QUERY -k k
For example: 
    $ python search.py -q Can dog speak?
Or: 
    $ python search.py -q Can dog speak? -k 3


## Presentation Link
    https://youtu.be/UsTpQRGptTY


## Reproduce Result
To reproduce our test cases, please first run the following if you haven't build an inverted index:
    $ python dataPreprocessing.py
Then, run the following:
    $ python search.py -q who is Obama?
    $ python search.py -q George H W Bushs?

To reproduce our experiment result, please first run the following if you haven't build an inverted index:
    $ python dataPreprocessing.py
Then, run the following:
    $ python evaluation.py


## Contributors
    - Yiquan Xiao
    - Weiqi Wang
    - Zihan Wang
    - Runge Huang


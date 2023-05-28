import numpy as np
import csv

class Index:
    def __init__(self):
        self.docs = []  # store all document names
        self.doc_lengths = []  # sotore doc length for each doc
        self.token_to_id = {}  # dictionary map each token to its id
        self.inverted_index = []  # 2d numpy array to store inverted index
    
    def build_inverted_index(self, file_path="inverted_index/inverted_index.csv"):
        """
        build an inverted index
        
        Parameters
        ----------
        file_path: str
            the path of the csv file that store inverted index info

        Returns
        -------
        """
        with open(file_path) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            num_tokens = 0
            for row in csv_reader:
                if num_tokens == 0:
                    self.docs = row[1:]
                else:
                    self.inverted_index.append(list(map(int, row[1:])))  # convert to integer and add to the inverted_index
                    self.token_to_id[row[0]] = num_tokens - 1
                num_tokens += 1
        
        self.inverted_index = np.array(self.inverted_index)  # convert to numpy array
        self.doc_lengths = np.sum(self.inverted_index, axis=0)  # get doc lengths
    
    def get_corpus_size(self):
        """
        get total number of documents in the corpus
        
        Parameters
        ----------

        Returns
        -------
        corpus_size: int
            total number of documents in the corpus
        """
        return len(self.docs)
    
    def get_avg_doc_length(self):
        """
        get average document length in the corpus
        
        Parameters
        ----------

        Returns
        -------
        avg_doc_length: float
            average document length in the corpus
        """
        return np.average(self.doc_lengths)
    
    def get_term_freq(self, token, doc_id):
        """
        get term frequency (number of occurrence) of the token in the document
        
        Parameters
        ----------
        token: str
            token to be checked
        doc_id: int
            id of the document
        
        Returns
        -------
        term_freq: int
            term frequency (number of occurrence) of the token in the document
        """
        # if token not in the inverted index, the corresponding term frequency is just 0
        if token not in self.token_to_id:
            return 0
        
        token_id = self.token_to_id[token]  # get token id
        return self.inverted_index[token_id, doc_id]
    
    def get_doc_length(self, doc_id):
        """
        get document length
        
        Parameters
        ----------
        doc_id: int
            id of the document
        
        Returns
        -------
        doc_length: int
            document length
        """
        return self.doc_lengths[doc_id]
    
    def get_doc_freq(self, token):
        """
        get document frequency of the token
        i.e. number of document contains the token
        
        Parameters
        ----------
        token: str
            token to be checked
        
        Returns
        -------
        doc_freq: int
            number of document contains the token
        """
        # if token not in the inverted index, the corresponding term frequency is just 0
        if token not in self.token_to_id:
            return 0
        token_id = self.token_to_id[token]  # get token id
        doc_freq = (self.inverted_index[token_id] > 0).sum() 
        return doc_freq


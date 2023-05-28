import math
from index import Index
from dataPreprocessing import tokenizer
from collections import defaultdict
from heapq import nlargest


class BM25:
    def __init__(self, indexer=None, k1=1.2, k2=5, b=0.75):
        if indexer is None:
            self.indexer = Index()
            self.indexer.build_inverted_index()
        else:
            self.indexer = indexer
        
        self.k1 = k1  # k1 = 1.2 works well in practice
        self.k2 = k2  # k2 varies from 0 to 1000
        self.b = b  # b = 0.75 works well in practice
        self.avg_doc_len = self.indexer.get_avg_doc_length()  # avg doc length in the corpus
        self.num_docs = self.indexer.get_corpus_size()  # number of docs in the corpus

    def score_query(self, term_freq, query_freq, doc_freq, doc_len):
        """
        give a score of a specific query token for a document
        
        Parameters
        ----------
        term_freq: int 
            term frequence of the query token in the document
        query_freq: int 
            term frequence of the query token in the query
        doc_freq: int 
            document frequence of the query token 
            i.e. number of document contains the query token
        doc_len: int 
            length of the document
        
        Returns
        -------
        score: float
            score of a specific query token for a document
        """
        k = self.k1 * ((1 - self.b) + self.b * doc_len / self.avg_doc_len)
        idf_part = math.log((self.num_docs - doc_freq + 0.5) / (doc_freq + 0.5))
        doc_part = ((self.k1 + 1) * term_freq) / (k + term_freq)
        query_part = ((self.k2 + 1) * query_freq) / (self.k2 + query_freq)
        return idf_part * doc_part * query_part
    
    def retrieve_doc(self, query, k):
        """
        retrieve the most relevant k documents from the corpus given the query
        
        Parameters
        ----------
        query: str
            user's query
        k: int
            top k documents (most relevant k documents) to show
        
        Returns
        -------
        top_k_docs: list
            a list of the most relevant documents, ordered by relevance 
        """
        # store term frequencies for each query token
        query_freqs = defaultdict(int)  # dictionary with default value 0
        for word in query.split():
            query_token = tokenizer(word)
            query_freqs[query_token] += 1
        
        # calculate score for each document
        doc_scores = defaultdict(float)  # dictionary with default value 0.0
        for doc_id in range(self.num_docs):
            for query_token in query_freqs:
                if query_token == '':  # we don't care about punctuations
                    continue
                
                term_freq = self.indexer.get_term_freq(query_token, doc_id)
                query_freq = query_freqs[query_token]
                doc_freq = self.indexer.get_doc_freq(query_token)
                doc_len = self.indexer.get_doc_length(doc_id)
                
                doc_scores[doc_id] += self.score_query(term_freq, query_freq, doc_freq, doc_len)
        
        # retrieve the top k document
        top_k_docs_ids = nlargest(k, doc_scores, key=doc_scores.get)
        top_k_docs = [self.indexer.docs[id]+".txt" for id in top_k_docs_ids]
        return top_k_docs




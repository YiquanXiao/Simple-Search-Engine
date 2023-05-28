import argparse
from retrievalModel import BM25
from index import Index


def run_query(query, k):
    model = BM25()
    retrieved_docs = model.retrieve_doc(query, k)
    print(f"Below are {k} most relevant documents for your query:")
    print(retrieved_docs)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    # add arguments
    parser.add_argument("-q", "--query", nargs="*", required=True, 
                        help="The query you want to ask", dest="query")
    parser.add_argument("-k", "--topk", type=int, default=5, 
                        help="k most revelant documents will be shown", dest="k")
    
    # parse the argument
    args = parser.parse_args()
    
    # retrieve document based on the query
    run_query(" ".join(args.query), args.k)

from retrievalModel import BM25


def precision(retrieved_docs, relevant_docs):
    tp = 0  # true positive
    for doc in retrieved_docs:
        if doc in relevant_docs:
            tp += 1
    return tp / len(retrieved_docs)


def recall(retrieved_docs, relevant_docs):
    tp = 0  # true positive
    for doc in retrieved_docs:
        if doc in relevant_docs:
            tp += 1
    return tp / len(relevant_docs)


if __name__ == '__main__':
    # set query and mark all relevant documents for this query
    query1 = "who is Obama? "
    relevant_docs1 = ["Barack Obama.txt","Barack Obama Early Life.txt","Barack Obama Education.txt","Barack Obama Legislative career.txt","Barack Obama Presidential campaigns.txt"]
    k1 = 5
    
    query2 = "who is George H W Bushs?"
    relevant_docs2 = ["George H W  Bush Vice Presidency.txt","George H W Bush Early Life.txt","George H W Bush President Election.txt","George H W Bush WW2.txt","George H W Bush.txt"]
    k2 = 5
    
    model = BM25()
    retrieved_docs1 = model.retrieve_doc(query1, k1)
    retrieved_docs2 = model.retrieve_doc(query2, k2)
    
    print("Query1:", f"'{query1}'")
    print(f"Precision@{k1}:", precision(retrieved_docs1, relevant_docs1))
    print(f"Recall@{k1}:", recall(retrieved_docs1, relevant_docs1))
    print("-" * 100)
    
    print("Query2:", f"'{query2}'")
    print(f"Precision@{k2}:", precision(retrieved_docs2, relevant_docs2))
    print(f"Recall@{k2}:", recall(retrieved_docs2, relevant_docs2))
    print("-" * 100)



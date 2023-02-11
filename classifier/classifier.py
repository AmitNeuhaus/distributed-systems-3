import numpy as np
from sklearn.tree import DecisionTreeClassifier
from sklearn.model_selection import train_test_split

# preparing vectors vectors
patterns_amount_file = open("../patterns-amount.txt", "r")
patterns_amount = int(patterns_amount_file.read())
print(patterns_amount)
patterns_amount_file.close()


nouns_vectors_file = open("../final/part-r-00000", "r")
noun_vectors = nouns_vectors_file.readlines()

vectors = []

for line in noun_vectors:
    vector= np.zeros((patterns_amount), dtype=int)
    print(vector)
    splitted = line.strip("\n").split("\t")
    nouns = splitted[0]
    patterns = splitted[1].split(",")
    for pattern in patterns:
        print(pattern)
        pattern_index = int(pattern.split("-")[0])
        pattern_count = int(pattern.split("-")[1])
        vector[pattern_index] = pattern_count
    vectors.append(vector)

labels=np.array([True,True])
    
    






# X = np.array([[0, 0, 0, 0], [0, 0, 0, 1], [0, 1, 0, 0], [1, 0, 0, 0]])
# y = np.array([False, True, True, False])

# Split the data into training and testing sets
vectors_train, vectors_test, labels_train, labels_test = train_test_split(vectors, labels, test_size=0.2, random_state=0)

# Create and train a Decision Tree Classifier
clf = DecisionTreeClassifier()
clf.fit(vectors_train, labels_train)

# Evaluate the classifier on the test data
accuracy = clf.score(vectors_test, labels_test)
print("Accuracy:", accuracy)

# Predict the labels for new, unseen data
X_new = np.array([[0, 3]])
y_pred = clf.predict(X_new)
print("Predicted label:", y_pred[0])

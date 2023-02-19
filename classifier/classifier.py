import numpy as np
from sklearn.tree import DecisionTreeClassifier
from sklearn.model_selection import KFold, cross_val_score
from sklearn.metrics import precision_score, recall_score, f1_score

# preparing vectors vectors
patterns_amount_file = open("../patterns-amount.txt", "r")
patterns_amount = int(patterns_amount_file.read())
patterns_amount_file.close()


nouns_vectors_file = open("../LABLED_DATA/part-r-00000", "r")
noun_vectors = nouns_vectors_file.readlines()

vectors = []
labels = []

for line in noun_vectors:
    vector = np.zeros((patterns_amount), dtype=int)
    splitted = line.strip("\n").split("\t")
    nouns = splitted[0]
    patterns = splitted[1].split(":")[0].split(",")
    label = splitted[1].split(":")[1]
    label = True if label == "True" else False
    for pattern in patterns:
        pattern_index = int(pattern.split("-")[0])
        pattern_count = int(pattern.split("-")[1])
        vector[pattern_index] = pattern_count
    labels.append(label)
    vectors.append(vector)


# Create and train a Decision Tree Classifier
clf = DecisionTreeClassifier()
k_fold = KFold(n_splits=10, shuffle=True, random_state=42)
scores = cross_val_score(clf, vectors, labels, cv=k_fold)

print("Finished train")

# Evaluate the classifier on the test data
print("Accuracy: %0.2f (+/- %0.2f)" % (np.mean(scores), np.std(scores) * 2))

clf.fit(vectors, labels)
# Predict the labels for new, unseen data

# Use the classifier to predict labels for a new set of vectors
predicted_labels = clf.predict(vectors)

# Calculate precision, recall, and F1 scores
precision = precision_score(labels, predicted_labels)
recall = recall_score(labels, predicted_labels)
f1 = f1_score(labels, predicted_labels)
example_vector = np.array([vectors[0]])
y_pred = clf.predict(example_vector)
# Print the results
print("Precision: {:.2f}".format(precision))
print("Recall: {:.2f}".format(recall))
print("F1 score: {:.2f}".format(f1))
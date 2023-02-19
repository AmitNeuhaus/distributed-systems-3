import numpy as np
from sklearn.tree import DecisionTreeClassifier
from sklearn.model_selection import KFold, cross_val_score

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

example_vector = np.array([vectors[0]])
y_pred = clf.predict(example_vector)
print("Predicted label:", y_pred[0], labels[0])

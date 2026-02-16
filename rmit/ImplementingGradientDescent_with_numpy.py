# ===============================================
# Gradient Descent Implementation (Single Neuron)
# Admissions Dataset – Logistic Regression Style
# ===============================================

import numpy as np
from data_prep import features, targets, features_test, targets_test

# ------------------------------------------------
# Sigmoid Activation Function
# ------------------------------------------------
# Converts any real number into a value between 0 and 1.
# Used because this is a binary classification problem.
#
# Formula:
#   sigmoid(h) = 1 / (1 + e^(-h))
#
# Why?
# - Produces probabilities
# - Smooth and differentiable (needed for gradient descent)
# ------------------------------------------------
def sigmoid(x):
    return 1 / (1 + np.exp(-np.array(x, dtype=float)))


# ------------------------------------------------
# Derivative of Sigmoid
# ------------------------------------------------
# Needed for backpropagation / gradient descent.
#
# If:
#   f(h) = sigmoid(h)
#
# Then:
#   f'(h) = f(h)(1 - f(h))
#
# IMPORTANT:
# Instead of recomputing sigmoid(h),
# we reuse the already computed output.
# ------------------------------------------------
def sigmoid_prime(x):
    return sigmoid(x) * (1 - sigmoid(x))


# ------------------------------------------------
# Reproducibility
# ------------------------------------------------
# Setting random seed ensures:
# - Same random weights every run
# - Easier debugging
# ------------------------------------------------
np.random.seed(42)


# ------------------------------------------------
# Dataset shape
# ------------------------------------------------
# n_records  = number of training samples (rows)
# n_features = number of input features (columns)
#
# In this dataset:
# Features = [GRE, GPA, rank1, rank2, rank3, rank4]
# So n_features = 6
# ------------------------------------------------
n_records, n_features = features.shape

last_loss = None


# ------------------------------------------------
# Weight Initialization
# ------------------------------------------------
# We initialize weights from a normal distribution
# centered at 0.
#
# scale = 1 / sqrt(n_features)
#
# Why?
# - Keeps initial weighted sum (h) small
# - Prevents sigmoid saturation
# - Encourages stable training
# ------------------------------------------------
weights = np.random.normal(scale=1 / n_features**.5, size=n_features)


# ------------------------------------------------
# Hyperparameters
# ------------------------------------------------
# epochs     = how many full passes over the dataset
# learnrate  = learning rate (step size)
#
# Larger learning rate = faster updates but risk of divergence
# Smaller learning rate = slower but stable
# ------------------------------------------------
epochs = 1000
learnrate = 0.5


# ======================================================
#                 TRAINING LOOP
# ======================================================
# We repeat weight updates for multiple epochs
# Each epoch = one full pass through training data
# ======================================================
for e in range(epochs):

    # ------------------------------------------------
    # Initialize weight update accumulator to zero
    # Δw_i = 0
    #
    # This will accumulate weight updates for
    # all records in this epoch.
    # ------------------------------------------------
    del_w = np.zeros(weights.shape)

    # ------------------------------------------------
    # Loop through each training example
    # x = feature vector
    # y = true label (0 or 1)
    # ------------------------------------------------
    for x, y in zip(features.values, targets):

        # Ensure numeric type (avoid dtype object issues)
        x = x.astype(np.float64)

        # ------------------------------------------------
        # Forward Pass
        # ------------------------------------------------

        # Compute weighted sum:
        # h = Σ(w_i * x_i)
        h = np.dot(x, weights)

        # Apply sigmoid to get predicted probability
        output = sigmoid(h)

        # ------------------------------------------------
        # Compute Error
        # ------------------------------------------------
        # error = actual - predicted
        error = y - output

        # ------------------------------------------------
        # Compute Error Term (δ)
        # ------------------------------------------------
        # δ = error * sigmoid'(h)
        #
        # Since:
        # sigmoid'(h) = output * (1 - output)
        #
        # We reuse output for efficiency
        error_term = error * output * (1 - output)

        # ------------------------------------------------
        # Accumulate weight changes
        # ------------------------------------------------
        # Δw_i += δ * x_i
        #
        # Each weight is updated proportional to:
        # - how wrong we are (error)
        # - how sensitive sigmoid is
        # - how large input is
        # ------------------------------------------------
        del_w += error_term * x

    # ------------------------------------------------
    # Update Weights (After processing all records)
    # ------------------------------------------------
    # w_i = w_i + η * Δw_i / m
    #
    # We divide by n_records (m) to compute the mean
    # weight update (since we're using MSE).
    # ------------------------------------------------
    weights += learnrate * del_w / n_records


    # ------------------------------------------------
    # Monitor Training Loss
    # ------------------------------------------------
    # Every 10% of epochs, print MSE
    # ------------------------------------------------
    if e % (epochs / 10) == 0:

        # Forward pass for all training data
        out = sigmoid(np.dot(features, weights))

        # Compute Mean Squared Error
        loss = np.mean((out - targets) ** 2)

        # Check if loss is increasing
        if last_loss and last_loss < loss:
            print("Train loss:", loss, "WARNING - Loss Increasing")
        else:
            print("Train loss:", loss)

        last_loss = loss


# ======================================================
#               TEST SET EVALUATION
# ======================================================

# Forward pass on test data
tes_out = sigmoid(np.dot(features_test, weights))

# Convert probabilities to class predictions
predictions = tes_out > 0.5

# Calculate accuracy
accuracy = np.mean(predictions == targets_test)

print(f"Prediction accuracy: {accuracy:.3f}")

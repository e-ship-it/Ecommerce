from sklearn.neighbors import NearestNeighbors
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, precision_score, recall_score
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import joblib

# Save the trained KNN model and other necessary components
def save_model(knn, filename='knn_model.pkl'):
    joblib.dump(knn, filename)  # Save the model to a file
    print(f"Model saved to {filename}")


# Function for predicting product ratings using nearest neighbors
def predict_rating(train_data, user_id, product_name, knn, train_matrix):
    # Ensure user_id exists in the train data
    if user_id not in train_data['user_id'].values:
        return 0  # If user_id is not found, return 0
    
    # Get the index of the user in the train data
    user_idx = train_data[train_data['user_id'] == user_id].index[0]
    
    # Get the nearest neighbors
    distances, indices = knn.kneighbors(train_matrix.iloc[user_idx].values.reshape(1, -1))
    
    # Initialize a list to store predicted ratings for the product
    recommended_ratings = []

    # Iterate through neighbors and get their ratings for the target product
    for i in indices[0]:
        recommended_ratings.append(train_matrix.iloc[i][product_name])
    
    # Return the average rating from neighbors (or 0 if no ratings)
    return np.mean(recommended_ratings) if recommended_ratings else 0

# Function for recommending top N products for a given user
def recommend_top_n_products(train_data, user_id, knn, train_matrix, n=5):
    # Ensure the user_id exists in the train data
    if user_id not in train_data['user_id'].values:
        return []  # Return empty list if user_id not found
    
    # Get the index of the user in the train data
    user_idx = train_data[train_data['user_id'] == user_id].index[0]
    
    # Get the nearest neighbors
    distances, indices = knn.kneighbors(train_matrix.iloc[user_idx].values.reshape(1, -1))
    
    # Collect the products and their ratings from nearest neighbors
    product_ratings = {}
    
    for i in indices[0]:
        # Get the products rated by the neighbor
        for product in train_matrix.columns:
            rating = train_matrix.iloc[i][product]
            if rating > 0:  # Consider only products that the neighbor has rated
                if product not in product_ratings:
                    product_ratings[product] = []
                product_ratings[product].append(rating)
    
    # Average the ratings for each product from the nearest neighbors
    avg_ratings = {product: np.mean(ratings) for product, ratings in product_ratings.items()}
    
    # Sort products by average rating in descending order
    sorted_products = sorted(avg_ratings.items(), key=lambda x: x[1], reverse=True)
    
    # Return the top N recommended products
    top_n_products = sorted_products[:n]
    return top_n_products

# Function to calculate Precision@k
def precision_at_k(y_true, y_pred, k=3):
    top_k_preds = np.argsort(y_pred)[::-1][:k]
    relevant_items = [1 if item in y_true else 0 for item in top_k_preds]
    return np.sum(relevant_items) / k

# Load environment variables
cwd_ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(dotenv_path = cwd_ + '/.env')

# Database connection
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")

engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")

# STEP 1: Load data
df = pd.read_sql("select * from mart.order_summary", engine)
# STEP 2: Create User-Item Interaction Matrix
interaction_matrix = df.pivot_table(index='user_id', columns='product_name', values='reordered', aggfunc='sum', fill_value=0).reset_index()

# STEP 3: Train/Test Split
train_data, test_data = train_test_split(interaction_matrix, test_size=0.2, random_state=42)

# STEP 4: Build KNN Model
knn = NearestNeighbors(metric='cosine', algorithm='brute', n_neighbors=5)

# Train the KNN model on the interaction matrix (excluding 'user_id')
train_matrix = train_data.drop('user_id', axis=1)
knn.fit(train_matrix)

# STEP 5: Example Prediction
# Predict the rating for a user and product
user_id = 7864
product_name = 'Purified Water'

# Make prediction
predicted_rating = predict_rating(train_data, user_id, product_name, knn, train_matrix)
print(f"Predicted Rating for user {user_id} on product '{product_name}': {predicted_rating}")

top_5_recommendations = recommend_top_n_products(train_data, user_id, knn, train_matrix, n=5)
print(f"Top 5 recommended products for user {user_id}:")
for idx, (product, rating) in enumerate(top_5_recommendations):
    print(f"{idx+1}. {product} - Predicted Rating: {rating}")


save_model(knn)    
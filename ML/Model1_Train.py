from sklearn.datasets import load_iris
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.naive_bayes import MultinomialNB
from sklearn.svm import SVC
from sklearn.metrics import classification_report, confusion_matrix
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
import os
import joblib


# Load CSV file
cwd_ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# Step 1: Load environment variables from .env file
load_dotenv(dotenv_path = cwd_ + '/.env')
# Step 2: Get the connection parameters from the environment
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")

engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")
# STEP 1: Read a dbt model
df_dim_product = pd.read_sql("SELECT * FROM opts_hub.dim_product", engine)
print(len(df_dim_product),df_dim_product.head(2).transpose())

# Step 2: Feature and label
X = df_dim_product['product_name']           # Text feature
y = df_dim_product['department']       # Target label


# Step 3: Vectorize product names
vectorizer = TfidfVectorizer()
X_vectorized = vectorizer.fit_transform(X)

#X, y = load_iris(return_X_y=True)
X_train, X_test, y_train, y_test = train_test_split(X_vectorized, y, test_size=0.2, random_state=42)

# Models to compare
models = {
    "Logistic Regression": LogisticRegression(max_iter=200),
    "Decision Tree": DecisionTreeClassifier(),
    "Random Forest": RandomForestClassifier(),
    "KNN": KNeighborsClassifier(),
    "SVM": SVC(),
    "MultinomialNB": MultinomialNB()
}

model_acc = dict()
trained_models = {}
# Train and evaluate
with open("Classification_Nodel_product_dptmt.txt",'w') as f:
    for name, model in models.items():
        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)
        acc = accuracy_score(y_test, y_pred)
        model_acc[name] = acc
        trained_models[name] = model
        
        # Write nicely formatted output
        f.write(f"Model: {name}\n")
        f.write(f"Accuracy: {acc:.2f}\n")
        f.write("Confusion Matrix:\n")
        f.write(f"{confusion_matrix(y_test, y_pred)}\n")
        f.write("Classification Report:\n")
        f.write(f"{classification_report(y_test, y_pred, zero_division=0)}\n")
        f.write("="*50 + "\n")


    # choosing the best model
    best_model_name = max(accuracies, key=accuracies.get)
    best_model = trained_models[best_model_name]
    joblib.dump(best_model, 'Classification_Nodel_product_dptmt_best.joblib')



import joblib


# load the model
model = joblib.dump(model, 'model.joblib')
#check the prediction
new_product = ["Fresh Banana"]
# Vectorize new input using the SAME vectorizer
X_new = vectorizer.transform(new_product)
# Predict department
predicted_department = model.predict(X_new)
print(predicted_department)  # Output: 'Produce' (for example)

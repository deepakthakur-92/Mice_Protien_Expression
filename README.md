# Mice Protien Expression

Protein Expression classification models are frequently viewed not only as difficult
task, but also as a classification problem that, in some cases, requires a trade-off
between accuracy and efficiency in analysis  validation due to the large amount of data
available.
Expression levels of 77 proteins measured in the cerebral cortex of 8 classes of control
and Down syndrome mice exposed to content fear conditioning, a task used to access
associative learning.
The aim is to identify subsets of proteins that are discriminant between the classes.
Basically, this is multi-class classification problem

# Dataset

The data set includes the expression levels of 77 proteins/protein changes that genearated 
measurable signals in the cortex's nuclear fraction. There are 72 mice in all, with 38 control
mice and 34 trisomic mice(Down syndrome). In the experiments, 15 measurements of each protein 
per sample/mouse were recorded. As a result, there are 38x15, or 570 measurement for control
mice and 34x15, 510 measurements for trisomic mice. There are 1080 measurements per protein 
in the dataset. Each measurement can be thought of as a separate sample/mouse.
link: https://archive.ics.uci.edu/ml/datasets/Mice+Protein+Expression

# Project Demo
[link](https://www.youtube.com/watch?v=BdcBIAsjWZs)


# Documents
[Architecture/LLD/HLD](https://drive.google.com/drive/folders/1bN_1kdMB6YZK-4jB6hRMQyoCZR3ufYpJ)

# Database
MYSQL database has been used to store training, prediction dataset and for logging

# Installation
## :hammer_and_wrench: Requirements
- python 3.x
- Flask
- pandas
- mysql-connector-python
- kneed
- scikit-learn
- xgboost

# Contributer
- Deepak Thakur

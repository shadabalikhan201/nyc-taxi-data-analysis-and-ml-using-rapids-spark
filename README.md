# Who am I:
- 👋 Hi, this is [@shadabalikhan201](https://github.com/shadabalikhan201/) !
- 🌱 Interested in scalable data-pipeline and developing pyspark based gpu optimised data analysis and ml workflows.

# About this project:
- This is a gpu accelerated machine learning dashboard based poc that runs end to end on Nvidia's RTX gpu. pySpark is used for data analysis and machine learning whereas Plotly Dash is used for the creation of a reactive dashboard.

# Technologies Used:

#### Programming Languages:
- Python is used to code the entire stack, pySpark for transformations and Plotly Dash for frontend.

#### Development Environment:

1. Create the following conda environment with cudatoolkit, cudf, and cuml:
  
        conda create -n spark_rapids_23.06 -c rapidsai -c conda-forge -c nvidia cudf=23.06 cuml=23.06 python=3.9 cudatoolkit=11.5

2. Active the created environment:

       source activate /home/optimus_prime/anaconda3-2023/envs/spark_rapids_23.06

3. Install following python packages:
                     
       1.  pip install py4j
        
       2.  pip install pyspark==3.4.0

       3.  pip install findspark

       4.  pip install numpy
    
       5.  pip install pandas
    
       6.  pip install scikit-learn
    
       7.  pip install spark-rapids-ml
    
       8.  pip install delta-spark
    
       9.  pip install flask
    
       10. pip install dash
    
       11. pip install dash-bootstrap-components


USE [tpcxbb_1gb]
GO

-- An attempt at predicting review valence on the Microsoft product purchase database

-- Cite your sources:
-- Heavily inspired by and using the dataset of: https://microsoft.github.io/sql-ml-tutorials/python/customerclustering/
-- NLP Regression methodology and workflow based off of tutorials from: https://azure.github.io/learnAnalytics-MicrosoftML/
-- General TSQL help and some :https://www.microsoft.com/en-us/learning/course.aspx?cid=20761

DROP TABLE IF EXISTS [dbo].[model]
GO
CREATE TABLE [dbo].[model](
 [language] [varchar](30) NOT NULL,
 [model_name] [varchar](30) NOT NULL,
 [model] [varbinary](max) NOT NULL,
 [time] [datetime2](7) NULL DEFAULT (sysdatetime()),
 [user] [nvarchar](500) NULL DEFAULT (suser_sname())
)
GO

-- I was trying to implement cross validation here, but I can't get it working right
-- The way I had it set up was using a procedure to create new views for each fold, but I couldn't get it working right
-- I think it might possibly be easier to do with python? I'll probably ask my TA

CREATE OR ALTER VIEW pr_training_data
AS
SELECT TOP(CAST( ( SELECT COUNT(*) FROM   product_reviews)*0.9 AS INT))
  CAST(pr_review_content AS NVARCHAR(4000)) AS review_content,
  CASE 
   WHEN pr_review_rating <3 THEN 0 
   WHEN pr_review_rating =3 THEN 1 
   ELSE 2 
  END AS valence 
FROM product_reviews;
GO

CREATE OR ALTER VIEW pr_test_data
AS
SELECT TOP(CAST( ( SELECT COUNT(*) FROM   product_reviews)*0.1 AS INT))
  CAST(pr_review_content AS NVARCHAR(4000)) AS review_content,
  CASE 
   WHEN pr_review_rating <3 THEN 0 
   WHEN pr_review_rating =3 THEN 1 
   ELSE 2 
  END AS valence 
FROM product_reviews;
GO

-- Procedure to create and train model
CREATE OR ALTER PROCEDURE [dbo].[train_log]
AS
BEGIN
 DECLARE @model varbinary(max), @train_script nvarchar(max);

 SET @train_script = N'
from microsoftml import rx_logistic_regression as log,featurize_text as featurize, n_gram
import pickle as p

training_data["valence"] = training_data["valence"].astype("category")

model = log(formula = "valence ~ predictors", 
			data = training_data,
			method = "multiClass", 
			ml_transforms=[
				featurize(language="English", 
						  cols=dict(predictors="review_content"), 
				          word_feature_extractor=n_gram(2, weighting="TfIdf")
						  )
						  #Note: Can not get stopwordsDefault working
				]
		   )
		   #Note: I would normally use cross validation to choose the right ngram size and weighting
		   #but that really is not computationaly feasible here
bin = p.dumps(model)';

 EXECUTE sp_execute_external_script
      @language = N'Python',
      @script = @train_script,
      @input_data_1 = N'SELECT * FROM pr_training_data',
      @input_data_1_name = N'training_data',
      @params  = N'@bin varbinary(max) OUTPUT',
      @bin = @model OUTPUT;  
 DELETE FROM dbo.models WHERE language = 'Python';
 INSERT INTO dbo.models (language, model_name, model) VALUES('Python', 'logreg', @model);
END;
GO

-- Execute the procedure to store the model
EXECUTE [dbo].[train_log];
GO

-- Procedure to run the valence model on our test set
CREATE OR ALTER PROCEDURE [dbo].[valence_model]
AS
BEGIN
 DECLARE @model_binary varbinary(max), @test_script nvarchar(max);

 SET @model_binary = (select model from dbo.models WHERE model_name = 'logreg' and language = 'Python');
 
 SET @test_script = N'
from revoscalepy import rx_data_step as convert
from microsoftml import rx_predict as predict
import pickle as p

model = p.loads(model_binary)
predictions = predict(model = model, data = test, extra_vars_to_write = ["valence", "review_content"])
result = convert(predictions)';
 
 EXECUTE sp_execute_external_script
    @language = N'Python',
    @script = @test_script,
    @input_data_1 = N'SELECT * FROM pr_test_data',
    @input_data_1_name = N'test',
    @output_data_1_name = N'result',
    @params  = N'@model_binary varbinary(max)',
    @model_binary = @model_binary
  WITH RESULT SETS (("Review" NVARCHAR(MAX),"Valence" FLOAT, "%Negative" FLOAT, "%Zero" FLOAT, "%Positive" FLOAT));   
END
GO
-- Execute the model testing
EXECUTE [dbo].[valence_model] 
GO


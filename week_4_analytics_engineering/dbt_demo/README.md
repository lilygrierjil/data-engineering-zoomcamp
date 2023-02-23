Welcome to your new dbt project!

### Using the starter project

Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [dbt community](http://community.getbdt.com/) to learn from other analytics engineers
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices


## Code for Homework

Code can be found in the models folder. Queries are as follows:

Question 3
```
SELECT COUNT(*) FROM `dbt_lgrier.stg_fhv_tripdata`
WHERE EXTRACT(YEAR FROM pickup_datetime) = 2019;
```


Question 4

```
SELECT COUNT(*) FROM `dbt_lgrier_production.fact_fhv_trips`
WHERE EXTRACT(YEAR FROM pickup_datetime) = 2019;
```



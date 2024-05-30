-- Data Cleaning


SELECT *
FROM layoffs;


-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Null Values or blank values
-- 4. Remove Any Columns


CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;


SELECT *, 
row_number() OVER (
PARTITION BY company, industry, total_laid_off, percentage_laid_off, 'date') AS row_num
FROM layoffs_staging
;


With duplicate_cte AS
	(
	SELECT *, 
	row_number() OVER (
	PARTITION BY company,
    location, 
    industry, total_laid_off, percentage_laid_off, 'date',stage,
    country, funds_raised_millions) AS row_num
	FROM layoffs_staging
	)
Select *
From duplicate_cte
Where row_num > 1;


Select *
from layoffs_staging
where company = 'elemy';


With duplicate_cte AS
	(
	SELECT *, 
	row_number() OVER (
	PARTITION BY company,
    location, 
    industry, total_laid_off, percentage_laid_off, 'date',stage,
    country, funds_raised_millions) AS row_num
	FROM layoffs_staging
	)
Delete 
From duplicate_cte
Where row_num > 1;



CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


Select *
From layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *, 
	row_number() OVER (
	PARTITION BY company,
    location, 
    industry, total_laid_off, percentage_laid_off, 'date',stage,
    country, funds_raised_millions) AS row_num
	FROM layoffs_staging;


Select *
From layoffs_staging2
where row_num > 1 ;

Delete 
From layoffs_staging2
where row_num > 1 ;



Select *
From layoffs_staging2
;


-- Standardizing data

Select company, trim(Company)
From layoffs_staging2;


UPDATE layoffs_staging2
Set company = trim(Company);


Select distinct *
From layoffs_staging2
where industry LIKE 'Crypto%';

Select distinct industry
From layoffs_staging2
;


update layoffs_staging2
set industry = 'Crypto'
where industry LIke 'Crypto%';

Select distinct country
from layoffs_staging2
where country like 'United States%'
;



select distinct country, TRIM(TRAILING'.' FROM country)
from layoffs_staging2
order by 1; 





Update layoffs_staging2
set country = TRIM(TRAILING'.' FROM country)
where country Like 'United States%';


SELECT `date`, 
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

update layoffs_staging2
Set `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

SELECT `date`
FROM layoffs_staging2;


ALTER table layoffs_staging2
Modify column `date` DATE;

SELECT *
FROM layoffs_staging2
Where total_laid_off IS NULL
and percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2
where industry is null
OR industry = '' ;


SELECT 
    *
FROM
    layoffs_staging2
WHERE
    company like 'Bally%';



Select t1.industry, t2.industry
from layoffs_staging2 t1
JOIN layoffs_staging2 t2
	on t1.company = t2.company
where (t1.industry IS NULL or t1.industry = '')
and t2.industry is NOT NUll;


update layoffs_staging2
set industry = null
where industry = '';

update layoffs_staging2 t1
JOIN layoffs_staging2 t2
	on t1.company = t2.company
SET t1.industry = t2.industry
where t1.industry IS NULL
AND t2.industry IS NOT NULL;


Select distinct *
From layoffs_staging2;




SELECT *
FROM layoffs_staging2
Where total_laid_off IS NULL
and percentage_laid_off IS NULL;



delete
FROM layoffs_staging2
Where total_laid_off IS NULL
and percentage_laid_off IS NULL;


SELECT *
FROM layoffs_staging2;

Alter Table layoffs_staging2
DROP COLUMN row_num;


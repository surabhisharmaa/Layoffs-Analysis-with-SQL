-- Data cleaning

 SELECT *  FROM LAYOFFS;
 
 -- 1. REMOVING DUPLICATES
 -- 2. STANDTARDIZE DATA
 -- 3. LOOK AT BLANK/NULL VALUES
 -- 4. REMOVE ANY COLUMNS
 
CREATE TABLE LAYOFFS_STAGING 
LIKE LAYOFFS;

INSERT LAYOFFS_STAGING
SELECT * 
FROM LAYOFFS;

SELECT *, 
row_number() OVER(
partition by company, industry, total_laid_off, 'date') as row_num
FROM LAYOFFS_STAGING;

select * from layoffs_staging;

-- 
with duplicates_cte as 
(
SELECT *, 
row_number() OVER(
partition by company,location, industry, total_laid_off,stage, funds_raised_millions, 'date') as row_num
FROM LAYOFFS_STAGING
)
select * 
from duplicates_cte
where row_num >1;

-- checking 'Oda' and if it has duplicates
select * from layoffs_staging 
where company = 'Oda';
-- 
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

select * from layoffs_staging2;

INSERT INTO layoffs_staging2 
SELECT *,
row_number() OVER(
partition by company,location, industry, total_laid_off,stage, funds_raised_millions, 'date') as row_num
FROM LAYOFFS_STAGING;

select * from layoffs_staging2 
WHERE ROW_NUM>1;

DELETE 
from layoffs_staging2 
WHERE row_num > 1;


-- STANDARDIZING DATA

SELECT company, TRIM(company) 
from layoffs_staging2;

Update layoffs_staging2
set company = TRIM(company);

SELECT distinct industry 
from layoffs_staging2
order by 1;

-- Crypo and crypto currency and cryptocurrent are essentially the same and there is a blank and a null
SELECT * 
from layoffs_staging2
where industry LIKE 'Crypto%';

Update layoffs_staging2 
set industry = 'Crypto'
where industry LIKE 'Crypto%';

select distinct country
from layoffs_staging2
order by 1;

-- See a problem with United States and anothe united states. hence will check and update

Select distinct country 
from layoffs_staging2 
where country like 'United States%';

Update layoffs_staging2
set country = 'United States of America' -- country = TRIM(Trailing '.' FROM country)
where country like 'United States%';

select distinct stage
from layoffs_staging2
order by 1;
-- Null

select distinct country
from layoffs_staging2
order by 1;

Select * 
from layoffs_staging2;

-- date is text and we have to change that
Update   layoffs_staging2
set `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

alter table layoffs_staging2
modify column `date` DATE;

-- WORKING WITH NULL DATA
Select *
from layoffs_staging2
where total_laid_off is null 
and percentage_laid_off is null;
-- where both these columns are null are of no use for us

-- Now trying to populate industry nulls: 
Select *
from layoffs_staging2
where industry is null 
or industry = '';

Select *
from layoffs_staging2
where company = 'Airbnb';

SELECT T1.COMPANY,T2.COMPANY,T1.industry, T2.INDUSTRY
FROM layoffs_staging2 T1
JOIN layoffs_staging2 T2
	ON T1.company=T2.company AND T1.location=T2.location
WHERE (T1.industry IS NULL OR T1.INDUSTRY = '')
AND T2.industry IS NOT NULL;

UPDATE layoffs_staging2 
SET INDUSTRY = NULL
WHERE industry = '';

UPDATE layoffs_staging2 T1
JOIN layoffs_staging2 T2
	ON T1.company=T2.company
SET T1.INDUSTRY = T2.INDUSTRY
WHERE T1.industry IS NULL 
AND T2.industry IS NOT NULL;

SELECT * 
FROM layoffs_staging2
WHERE company = "Bally's Interactive";

SELECT * 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- GETTING RID OF NULL DATA ROWS OF TOTAL LAID OFF AND PERCENTAGE LAID OFF
DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * 
FROM layoffs_staging2;

-- DELETING THE COLUMN OF ROW NUM AS IT IS NO LONGER USEFULL 
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;










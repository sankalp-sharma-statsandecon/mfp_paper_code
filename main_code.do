cd "C:\Users\ssharm31\Dropbox\DissertationResearch\MFP paper\Data"

*Import spot price data
foreach i in corn soy {
	import excel "`i' spot prices.xlsx", sheet("spot_prices_`i'") firstrow clear
	save Full_`i', replace
	}

*Download county data for city identification
foreach i in Arkansas Colorado Georgia Illinois Indiana Iowa Kansas Kentucky Michigan Minnesota Missouri Nebraska North%20Dakota Ohio South%20Dakota {
	readhtmltable https://www.zipcodestogo.com/`i'/, v
	foreach var of varlist * {
		capture rename `var' `=strtoname(`var'[1])'
		}
	drop in 1 
	destring, replace 
	keep Zip_Code City County
	quietly bysort City:  gen dup = cond(_N==1,0,_n)
	drop if dup > 1
	gen state = "`i'"
	drop dup
	save `i'_counties, replace
	}
	
cd "C:\Users\ssharm31\Dropbox\DissertationResearch\MFP paper\Data\append_data"
! dir *.dta /a-d /b >filelist.txt
	
*Append data
clear
file open myfile1 using filelist.txt, read
file read myfile1 line
use `line'
save master_data, replace

file read myfile1 line
while r(eof)==0 { /* while you're not at the end of the file */
	append using `line'
	file read myfile1 line
}
file close myfile1
save master_data, replace

cd "C:\Users\ssharm31\Dropbox\DissertationResearch\MFP paper\Data\append_data"
use master_data, replace
drop if Zip_Code == .
replace state = "North Dakota" if state == "North%20Dakota"
replace state = "South Dakota" if state == "South%20Dakota"
rename City city
save master_data, replace

*Merge data
cd "C:\Users\ssharm31\Dropbox\DissertationResearch\MFP paper\Data"
foreach i in corn soy {
	use Full_`i', clear
	format %td quote_date
	gen year = year(quote_date)
	keep if year >= 2017
	merge m:m city state using "C:\Users\ssharm31\Dropbox\DissertationResearch\MFP paper\Data\append_data\master_data.dta", force
	drop _merge
	merge m:m city state using "C:\Users\ssharm31\Dropbox\DissertationResearch\MFP paper\Data\append_data\extra_cities_`i'.dta", force
	*Prepare date variables
	*gen date2 = date(quote_date, "MDY")
	replace County = county if County == ""
	drop _merge
	drop if basis == .
	collapse (mean) quote_price (mean) basis, by(County state year)
	sort state County year
	rename County county
	replace county = upper(county)
	replace state = upper(state)
	save 2018_2019_`i', replace
	}

**Test code**
/*
use Full_corn, clear
format %td quote_date
gen year = year(quote_date)
keep if year >= 2017
merge m:m city state using "C:\Users\ssharm31\Dropbox\DissertationResearch\MFP paper\Data\append_data\master_data.dta", force
drop _merge
merge m:m city state using "C:\Users\ssharm31\Dropbox\DissertationResearch\MFP paper\Data\append_data\extra_cities.dta", force
replace County = county if County == ""
drop _merge
drop if basis == .
collapse (mean) quote_price (mean) basis, by(County year state)
sort state County year
use "C:\Users\ssharm31\Dropbox\DissertationResearch\MFP paper\Data\append_data\master_data.dta", clear

**End test code**	
*/
	
import delimited planted_acreage_data, clear
preserve
*replace state = substr(state, 1, 1)+lower(substr(state,2,.))
*replace county = substr(county, 1, 1)+lower(substr(county,2,.))
foreach i in CORN SOYBEANS {
	keep if commodity == "`i'"
	save planted_acreage_data_`i', replace
	restore, preserve
	}
erase "planted_acreage_data_corn.dta"
erase "planted_acreage_data_soy.dta"
shell ren "planted_acreage_data_CORN.dta" "planted_acreage_data_corn.dta"
shell ren "planted_acreage_data_SOYBEANS.dta" "planted_acreage_data_soy.dta"

foreach i in corn soy {
	use 2018_2019_`i', clear
	merge 1:m county state year using planted_acreage_data_`i', force
	**save and edit manually*
	2018_2019_`i'_incomplete, replace
	}
**Have to manually fix or impute values because either the county spellings are incorrect or values are missing in source data. l
**after doing that - proceed**

foreach i in corn soy {
	use 2018_2019_`i'_extra, clear
	drop if value == ""
	destring value, replace ignore(",")
	ren value acres_`i'
	ren quote_price price_`i'
	ren basis basis_`i'
	save `i'_acres_prices, replace	
	}

**test code**
/*
use 2018_2019_corn, clear
merge 1:m county state year using planted_acreage_data_corn, force
**end code**	
*/
	
*Convert to word table
/*
putdocx begin 
putdocx paragraph
putdocx text ("Year _____")
reg fd_acres l1.fd_price if year == 2019, cluster(county)
putdocx table mytable = etable
putdocx save myreport.docx, replace
*/

*Import MFP data: 
foreach i in Corn Soybeans {
	import excel mfp_county, sheet(MFP_`i'_by_County) clear
	replace D = "`i'_2018" in 5
	replace E = "`i'_2019" in 5
	replace F = "`i'_2020" in 5
	drop in 1/4
	foreach var of varlist * {
		rename `var' `=strtoname(`var'[1])'
		}
	drop in 1
	destring, replace 
	rename State_Name state
	rename County_Name county
	replace state = upper(state)
	gen y2018 = 2018
	gen y2019 = 2019
	gen y2020 = 2020
	save mfp_county_`i', replace
	}
	
foreach i in Corn Soybeans {
	forvalues j = 2018/2020 {
		use mfp_county_`i', clear
		keep state county `i'_`j' y`j'
		rename `i'_`j' crops 
		rename y`j' year
		save mfp_`i'_`j', replace
	}
}
foreach i in Corn Soybeans {
	use mfp_`i'_2020, clear
	forval j = 2018/2019 {
		append using mfp_`i'_`j'
	}
	sort state county year
	save mfp_main_`i', replace
}

shell ren "mfp_main_Corn.dta" "mfp_main_corn.dta"
shell ren "mfp_main_Soybeans.dta" "mfp_main_soy.dta"

*Merge with acres data
foreach i in corn soy {
	use `i'_acres_prices, clear
	merge m:m state county year using mfp_main_`i'
	drop if _merge == 2
	sort state county year 
	gen state_county = state+"_"+county
	encode state_county, gen(statecounty)
	drop _merge
	ren crops mfp_`i'
	replace mfp_`i' = 0 if mfp_`i' == .
	xtset statecounty year
	gen fd_price_`i' = d.price_`i'*100
	gen fd_acres_`i' = d.acres_`i'
	save `i'_main, replace
	}

use soy_acres_prices, clear	

*Import production data
import delimited "C:\Users\ssharm31\Dropbox\DissertationResearch\MFP paper\Data\production data\production.csv", clear
gen stcnty = state+"_"+county
encode stcnty, gen(state_county)
destring value, replace ignore(",") force
ren value production
keep state_county stcnty year production commodity
save production, replace

*Specifications
use corn_main, clear
duplicates tag statecounty year, gen(isdup) 
edit if isdup
drop if isdup == 1
drop isdup

xtset statecounty
gen fd_price_soy = d.price_soy*100
gen fd_acres_soy = d.acres_soy

*Simple model 
foreach i in corn soy {
	use `i'_main, clear
	replace price_`i' = price_`i'*100
	replace mfp_`i' = mfp_`i'/1000
	forval j = 2019/2020 {
		xi: reg fd_acres_`i' l1.mfp_`i' l1.price_`i' i.state if year == `j', cluster(state)
		}
	}

use corn_main, clear
	
*3SLS: model 1
foreach i in corn soy {
	use `i'_main, clear
	replace commodity = lower(commodity)
	replace commodity = "`i'" if commodity == ""	
	merge m:m state_county year commodity using production
	drop if _merge == 2
	xtset statecounty year
	forval j = 2019/2020 {
		xi: reg3 (fd_acres_`i' l1.price_`i' l1.mfp_`i' i.state) (fd_price_`i' = l1.production i.state) (production = l1.acres_`i' i.state) if year == `j'
		}
	}

use corn_main, clear	
merge m:m state_county year using soy_main
save corn_main_extra, replace

use soy_main, clear	
merge m:m state_county year using corn_main
save soy_main_extra, replace

	
*3SLS: model 2
*corn
use corn_main_extra, clear
replace commodity = lower(commodity)
replace commodity = "`i'" if commodity == ""	
drop _merge
merge m:m state_county year commodity using production
drop if _merge == 1 | _merge == 2
drop _merge
keep if commodity == "corn"
drop if acres_corn == .
replace price_soy = price_soy*100
replace price_corn = price_corn*100
replace production = production/1000000
replace mfp_corn = mfp_corn/1000
replace acres_corn = acres_corn/1000
xtset statecounty year
forval z = 2019/2020 {
		xi: reg3 (fd_acres_corn price_corn l1.mfp_corn i.state) (price_corn = l1.price_corn l1.price_soy l1.mfp_corn l1.production i.state) ///		
		(production = l1.acres_corn l1.mfp_corn l1.price_corn i.state) if year == `z'
		}

*soy
use soy_main_extra, clear
replace commodity = lower(commodity)
replace commodity = "`i'" if commodity == ""	
drop _merge
merge m:m state_county year commodity using production
drop if _merge == 1 | _merge == 2
drop _merge
keep if commodity == "soybeans"
drop if acres_soy == .
xtset statecounty year
replace price_soy = price_soy*100
replace price_corn = price_corn*100
replace production = production/1000000
replace acres_soy = acres_soy/1000
replace mfp_soy = mfp_soy/1000
forval z = 2019/2020 {
		xi: reg3 (fd_acres_soy l1.price_soy l1.mfp_soy i.state) (price_soy = l1.price_soy l1.price_corn l1.mfp_soy l1.production i.state) ///		
		(production = l1.acres_soy l1.mfp_soy l1.price_soy i.state) if year == `z'
		}

ren fd_price fd_price1 
ren fd_acres fd_acres1
merge 1:m state county year using corn_main	
drop _merge
merge m:m stcnty year commodity using production
keep if commodity =="SOYBEANS"
keep if _merge == 3
sort state county year 
xtset state_county year
xi: reg fd_acres1 fd_price1 fd_price l1.crops l1.crops1 if year == 2019, cluster(state_county)
xi: reg fd_acres1 l1.crops1 fd_price1 if year == 2020, cluster(state_county)
xi: reg fd_acres l1.crops fd_price if year == 2019, cluster(state_county)
xi: ivreg2 fd_acres l1.crops (fd_price = fd_price1 l1.crops1) if year == 2019, cluster(state_county)
xi: ivreg2 fd_acres1 l1.crops1 (fd_price1 = fd_price l1.crops) if year == 2019, cluster(state_county) first

reg3 (fd_acres1 l1.crops1) (fd_price1 = fd_price l1.crops l1.production) if year == 2019

/*use mfp_county, clear
replace MFP_Crops_2019 = MFP_Crops_2019 + MFP2_Crops_2019
replace MFP_Dairy_Hogs_2019 = MFP_Dairy_Hogs_2019 + MFP2_Livestock_2019
replace MFP_Specialty_Crops_2019 = MFP_Specialty_Crops_2019 + MFP2_Specialty_2019
keep state county MFP_Crops_2019 MFP_Dairy_Hogs_2019 MFP_Specialty_Crops_2019 y2019
rename MFP_Crops_2019 crops 
rename MFP_Dairy_Hogs_2019 dairy_hogs 
rename MFP_Specialty_Crops_2019 s_crops
rename y2019 year
save mfp_county_2019, replace

use mfp_county, clear
replace MFP_Crops_2020 = MFP_Crops_2020 + MFP2_Crops
replace MFP_Dairy_Hogs_2020 = MFP_Dairy_Hogs_2020 + MFP2_Livestock
replace MFP_Specialty_Crops_2020 = MFP_Specialty_Crops_2020 + MFP2_Specialty
keep state county MFP_Crops_2020 MFP_Dairy_Hogs_2020 MFP_Specialty_Crops_2020 y2020
rename MFP_Crops_2020 crops 
rename MFP_Dairy_Hogs_2020 dairy_hogs 
rename MFP_Specialty_Crops_2020 s_crops
rename y2020 year
save mfp_county_2020, replace
append using mfp_county_2019
append using mfp_county_2018
sort state county year
drop in 1/3
save mfp_main, replace
*/

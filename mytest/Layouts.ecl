import Standard, address;
export Layouts := Module


  export Layout_In_part1 := record
     string5		ZIP_5							;
     string4		Route_Num						;
     string4		ZIP_4   						;
     string9		WALK_Sequence					;
     string10 	    STREET_NUM						;
     string2		STREET_PRE_DIRectional			;
     string28	    STREET_NAME						;
     string2		STREET_POST_DIRectional			;
     string4		STREET_SUFFIX					;
     string4		Secondary_Unit_Designator		;
     string8		Secondary_Unit_Number			;
     string1		Address_Vacancy_Indicator		;
     string1		Throw_Back_Indicator			;
     string1		Seasonal_Delivery_Indicator		;
     string5		Seasonal_Start_Suppression_Date	;
     string5		Seasonal_End_Suppression_Date	;
     string1		DND_Indicator					;
     string1		College_Indicator				;
     string10		College_Start_Suppression_Date	;
     string10		College_End_Suppression_Date	;
     string1		Address_Style_Flag				;
     string5		Simplify_Address_Count			;
     string1		Drop_Indicator					;
     string1		Residential_or_Business_Ind		;
     string2		DPBC_Digit						;
     string1		DPBC_Check_Digit				;
     string10		Update_Date						;
     string10		File_Release_Date				;
     string10		Override_file_release_date		;
     string3		County_Num						;
     string28		County_Name					    ;
     string28		City_Name						;
     string2		State_Code						;
     string2		State_Num						;
     string2		Congressional_District_Number	;
     string1		OWGM_Indicator					;
     string1		Record_Type_Code				;
     string10 		ADVO_Key						;
     string1		Address_Type					;
     string1		Mixed_Address_Usage				;
  end;
  
  export Layout_In_part2 := record
     string67		Filler							;
  end;
  
  export Layout_In := record
    Layout_In_part1;
	Layout_In_part2;
  end;
  
  
  export Layout_Slim := record
     string10 	    STREET_NUM						;
     string2		STREET_PRE_DIRectional			;
     string28	    STREET_NAME						;
     string2		STREET_POST_DIRectional			;
     string4		STREET_SUFFIX					;
     string4		Secondary_Unit_Designator		;
     string8		Secondary_Unit_Number			;
	 string5		ZIP_5							;
	 string28		City_Name						;
     string2		State_Code						;	 
end;

  
  export Layout_Slim_Clean := record
   Layout_Slim;
	 string 		  Address1;
	 string 			Address2;
	 Unsigned8		RawAID;
end; 

 export Layout_Common_Out_k := record
	 Layout_In_part1;
	 string8  date_first_seen := '';
	 string8  date_last_seen := '';
	 string8  date_vendor_first_reported := '';
	 string8  date_vendor_last_reported := '';
	 string8  VAC_BEGDT ;
     string8  VAC_ENDDT ;
     string8  MONTHS_VAC_CURR ;
     string8  MONTHS_VAC_MAX ;                
     string8  VAC_COUNT ;
	 address.Layout_Clean182.prim_range;
	 address.Layout_Clean182.predir;
	 address.Layout_Clean182.prim_name;
	 address.Layout_Clean182.addr_suffix;
	 address.Layout_Clean182.postdir;
	 address.Layout_Clean182.unit_desig;
	 address.Layout_Clean182.sec_range;
	 address.Layout_Clean182.p_city_name;
	 address.Layout_Clean182.v_city_name;
	 address.Layout_Clean182.st;
	 address.Layout_Clean182.zip;
	 address.Layout_Clean182.zip4;
	 address.Layout_Clean182.cart;
	 address.Layout_Clean182.cr_sort_sz;
	 address.Layout_Clean182.lot;
	 address.Layout_Clean182.lot_order;
	 address.Layout_Clean182.dbpc;
	 address.Layout_Clean182.chk_digit;
	 address.Layout_Clean182.rec_type;
	 string2		fips_county:='';
	 string3		county:='';
	 address.Layout_Clean182.geo_lat;
	 address.Layout_Clean182.geo_long;
	 address.Layout_Clean182.msa;
	 address.Layout_Clean182.geo_blk;
	 address.Layout_Clean182.geo_match;
	 address.Layout_Clean182.err_stat;
	 string2 src := '';  //new field
	end;
	
	
 export Layout_Common_Out := record
 Layout_Common_Out_k;
 	 Unsigned8	RawAID;
	 Unsigned8	cleanaid;
	 String1			addresstype;
	 String1			Active_flag;
	 Unsigned8		scrubsbits1;
	 Unsigned8		scrubsbits2;
 end;
  
 export Layout_CDS := record
     string1		Record_Type_Code				;
     string11		Record_Type_Description			;
     string4		Route_Num						;
     string16		Route_Description				;
     string9		WALK_Sequence					;
     string1		Address_Vacancy_Indicator		;
     string22		Address_Vacancy_Description		;
     string1		Throw_Back_Indicator			;
     string1		Seasonal_Delivery_Indicator		;
     string36		Seasonal_Delivery_Description	;
     string5		Seasonal_Start_Suppression_Date	;
     string5		Seasonal_End_Suppression_Date	;
     string1		DND_Indicator					;
     string1		College_Indicator				;
     string10		College_Start_Suppression_Date	;
     string10		College_End_Suppression_Date	;
     string1		Address_Style_Flag				;
     string1		Drop_Indicator					;
     string1		Residential_or_Business_Ind		;
     string58		Delivery_Type_Description		;
     string3		County_Num						;
     string28		County_Name					    ;
     string28		City_Name						;
     string2		State_Code						;
     string2		State_Num						;
     string2		Congressional_District_Number	;
     string1		Address_Type					;
     string1		Mixed_Address_Usage				;
	 boolean		Lookedup := false;
	end;

 
  export Layout_Autokeys := record

	  Standard.L_Address.base addr;
	  unsigned6 zero  := 0;
	  string1   blank := '';
  end;
 
	 
End;
	 
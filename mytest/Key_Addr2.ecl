import doxie, Data_Services, ut;

Layout_revised := record
Layouts.Layout_Common_Out_k and not src;
end;

advo_b := project(Files().Base.built(active_flag = 'Y'), transform(Layout_revised, self := left));
export key_addr2 := INDEX(advo_b, 
							 {st, v_city_name, prim_range, prim_name, sec_range},
							 {advo_b},
							 '~thor_data400::key::advo::' + doxie.Version_SuperKey + '::addr_search2');
							 

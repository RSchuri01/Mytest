import versioncontrol;
export Files(

	 string		pversion = ''
	,boolean	pUseOtherEnvironment = false

) :=
module

	shared lnames := Filenames(pversion,pUseOtherEnvironment);

	versioncontrol.macInputFileVersions(lnames.Input	,Layouts.Layout_In					,Input);
	versioncontrol.macBuildFileVersions(lnames.Base		,Layouts.Layout_Common_Out	,Base	);

	export File_In						:= Input.Sprayed	;
	export File_Cleaned_Base	:= Base.qa				;

end;
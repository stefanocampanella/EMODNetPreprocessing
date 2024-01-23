### A Pluto.jl notebook ###
# v0.19.36

using Markdown
using InteractiveUtils

# This Pluto notebook uses @bind for interactivity. When running this notebook outside of Pluto, the following 'mock version' of @bind gives bound variables a default value (instead of an error).
macro bind(def, element)
    quote
        local iv = try Base.loaded_modules[Base.PkgId(Base.UUID("6e696c72-6542-2067-7265-42206c756150"), "AbstractPlutoDingetjes")].Bonds.initial_value catch; b -> missing; end
        local el = $(esc(element))
        global $(esc(def)) = Core.applicable(Base.get, el) ? Base.get(el) : iv(el)
        el
    end
end

# ╔═╡ d5739304-8a32-4b10-a224-ea178f8f8313
using PlutoUI

# ╔═╡ 3c01262b-c4b6-4f44-9c19-f2631c453922
using TOML

# ╔═╡ f43b6400-b516-11ee-0ffd-ad6fe8cbd4d9
using NCDatasets

# ╔═╡ cb4a084a-3d03-4dff-91ad-dcab7366fbb6
using Dates

# ╔═╡ a2139158-3347-4aa5-8039-10cbf945352a
using JLD2

# ╔═╡ 71818093-b3f6-4a87-97f7-bdaca574ebab
md"""Settings file: $(@bind settingsfilepath TextField(default="../configs/nutrient_profiles.toml"))"""

# ╔═╡ 7a327f9b-5aff-4067-8def-0f7bed5863f9
settings = TOML.parsefile(settingsfilepath)

# ╔═╡ d48f2f00-586d-4237-8cb0-8becfae45de2
emodnet2022 = NCDataset(settings["io"]["input"], "r")

# ╔═╡ 54af094d-22b1-4a46-ac85-f01ff258ab0f
stringfromvec(cs) = match(r"[^\0]+", String(cs)).match

# ╔═╡ 548c49a4-68e4-4b31-b22d-fb2cbe38964f
cruise_id = map(stringfromvec, eachcol(emodnet2022["cruise_id"][:, :]))

# ╔═╡ 48a23f0d-c1fe-4a90-91b5-ca7dd7bd72a4
allowed_platforms = settings["filters"]["platforms"]

# ╔═╡ a2458f44-bedc-48ef-86c3-2367f82c3933
platform_type = map(stringfromvec, eachcol(emodnet2022["Platform_type"][:, :]))

# ╔═╡ f1b8e784-35f1-4169-8abb-7377f27fb97e
datetime = emodnet2022["date_time"][:]

# ╔═╡ 6d3e76f4-ba4f-4934-9750-adb837169564
latitude = emodnet2022["latitude"][:]

# ╔═╡ 1d0414a2-321d-4403-a06d-30d515edf3dc
longitude = emodnet2022["longitude"][:]

# ╔═╡ 9027800c-593b-4ff4-8150-78fc9ebb3c5b
depths = emodnet2022["Depth"][:, :]

# ╔═╡ 466261cc-4e50-4bc4-9088-ebba1698aa78
mask = let
	start_date, end_date = settings["filters"]["datetime"]
	latmin, latmax = settings["filters"]["latitude"]
	lonmin, lonmax = settings["filters"]["longitude"]
	depthmin, depthmax = settings["filters"]["depth"]
	m = trues(size(depths)...)
	for index in eachindex(IndexCartesian(), depths)
		if ismissing(depths[index]) || depths[index] < depthmin || depths[index] > depthmax
			m[index] = false
		else
			station = index[2]
			if platform_type[station] ∉ allowed_platforms
				m[index] = false
			elseif datetime[station] < start_date || datetime[station] > end_date
				m[index] = false
			elseif latitude[station] < latmin || latitude[station] > latmax
				m[index] = false
			elseif longitude[station] < lonmin || longitude[station] > lonmax
				m[index] = false
			end
		end
	end
	m
end

# ╔═╡ d44d080b-a3fa-40f7-9392-b4426eb88691
npoints = count(mask)

# ╔═╡ 53c2c958-d129-4377-b75a-2b3139069721
stations = map(x -> x[2], eachindex(IndexCartesian(), mask))[mask]

# ╔═╡ 647d91b3-e0c7-416e-a81c-31fc1f819e94
cruisenames, data = let
	data = Dict{String,Vector{Union{Missing, Float32}}}()
	
	coords = [("YYYY", datetime, year),
			  ("MM", datetime, month),
	          ("DD", datetime, day),
	          ("lat", latitude, identity),
	          ("lon", longitude, identity)]
	
	for (name, xs, f) in coords
		@info "Processing $name"
		data[name] = map(f, xs[stations])
	end

	@info "Processing depth"
	data["depth"] = depths[mask]

	@info "Processing idcampain"
	cruise = cruise_id[stations]
	cruisenames = unique(cruise)
	idcampain = map(name -> findfirst(==(name), cruisenames), cruise)
	data["idcampain"] = idcampain
	
	for var in settings["variables"]
		output_name = var["output"]

		if output_name ∉ keys(data)
			@info "Processing $output_name"
			input_name = get(var, "input", missing)
			if !ismissing(input_name) && haskey(emodnet2022, input_name)
				var_data = emodnet2022[input_name][:, :]
				if haskey(var, "qc")
				    allowed_qc = var["qc"]
					var_data_qc = emodnet2022[input_name * "_qc"][:, :]
					for n in eachindex(var_data)
						qc = var_data_qc[n]
						if ismissing(qc) || qc ∉ allowed_qc
							var_data[n] = missing
						end
					end
				end
				if haskey(var, "range")
					low, high = var["range"]
					for n in eachindex(var_data)
						datum = var_data[n]
						if !ismissing(datum) && (datum < low || datum > high)
							var_data[n] = missing
						end
					end
				end
				data[output_name] = var_data[mask]
			else
				data[output_name] = Vector{Union{Missing, Float32}}(missing, npoints)
			end
		end
	end
	
	cruisenames, data
end

# ╔═╡ 221de565-8042-49e7-b80a-5a5dc7865e45
function matfromstrings(strings)
	len = maximum(length, strings)
	strings = map(s -> rpad(s, len), strings)
	reduce(hcat, map(collect, strings))
end

# ╔═╡ 222a7a75-a5a5-491c-a4fa-b76b39ec6979
Dataset(settings["io"]["output"], "c") do ds
	varnames = [var["output"] for var in settings["variables"]]
	unitnames = [haskey(var, "unit") ? var["unit"] : var["output"] for var in settings["variables"]]
	data_matrix = reduce(hcat, (data[name] for name in varnames))
	
	defVar(ds, "Cruises", matfromstrings(cruisenames), ("lendataset", "nCruises"))
	defVar(ds, "DATA", data_matrix, ("nData", "nVars"), fillvalue=1.0e20)
	defVar(ds, "UNITS", matfromstrings(unitnames), ("lenunits", "nVars"))
	defVar(ds, "VARIABLES", matfromstrings(varnames), ("lenvars", "nVars"))
end

# ╔═╡ e5fcffc8-4171-40df-8d52-3be6e3bc354f
jldsave("emodnet_nutrients.jld2"; data)

# ╔═╡ 00000000-0000-0000-0000-000000000001
PLUTO_PROJECT_TOML_CONTENTS = """
[deps]
Dates = "ade2ca70-3891-5945-98fb-dc099432e06a"
JLD2 = "033835bb-8acc-5ee8-8aae-3f567f8a3819"
NCDatasets = "85f8d34a-cbdd-5861-8df4-14fed0d494ab"
PlutoUI = "7f904dfe-b85e-4ff6-b463-dae2292396a8"
TOML = "fa267f1f-6049-4f14-aa54-33bafae1ed76"

[compat]
JLD2 = "~0.4.45"
NCDatasets = "~0.14.0"
PlutoUI = "~0.7.55"
"""

# ╔═╡ 00000000-0000-0000-0000-000000000002
PLUTO_MANIFEST_TOML_CONTENTS = """
# This file is machine-generated - editing it directly is not advised

julia_version = "1.10.0"
manifest_format = "2.0"
project_hash = "5d0d82a2d8ba00f5faf93f30b5ec461d15b391c7"

[[deps.AbstractPlutoDingetjes]]
deps = ["Pkg"]
git-tree-sha1 = "c278dfab760520b8bb7e9511b968bf4ba38b7acc"
uuid = "6e696c72-6542-2067-7265-42206c756150"
version = "1.2.3"

[[deps.ArgTools]]
uuid = "0dad84c5-d112-42e6-8d28-ef12dabb789f"
version = "1.1.1"

[[deps.Artifacts]]
uuid = "56f22d72-fd6d-98f1-02f0-08ddc0907c33"

[[deps.Base64]]
uuid = "2a0f44e3-6c83-55bd-87e4-b1978d98bd5f"

[[deps.Bzip2_jll]]
deps = ["Artifacts", "JLLWrappers", "Libdl", "Pkg"]
git-tree-sha1 = "9e2a6b69137e6969bab0152632dcb3bc108c8bdd"
uuid = "6e34b625-4abd-537c-b88f-471c36dfa7a0"
version = "1.0.8+1"

[[deps.CFTime]]
deps = ["Dates", "Printf"]
git-tree-sha1 = "ed2e76c1c3c43fd9d0cb9248674620b29d71f2d1"
uuid = "179af706-886a-5703-950a-314cd64e0468"
version = "0.1.2"

[[deps.ColorTypes]]
deps = ["FixedPointNumbers", "Random"]
git-tree-sha1 = "eb7f0f8307f71fac7c606984ea5fb2817275d6e4"
uuid = "3da002f7-5984-5a60-b8a6-cbb66c0b333f"
version = "0.11.4"

[[deps.CommonDataModel]]
deps = ["CFTime", "DataStructures", "Dates", "Preferences", "Printf", "Statistics"]
git-tree-sha1 = "349d9b36250ec19a7da18f838434db7d76c2f131"
uuid = "1fbeeb36-5f17-413c-809b-666fb144f157"
version = "0.3.2"

[[deps.Compat]]
deps = ["TOML", "UUIDs"]
git-tree-sha1 = "75bd5b6fc5089df449b5d35fa501c846c9b6549b"
uuid = "34da2185-b29b-5c13-b0c7-acf172513d20"
version = "4.12.0"
weakdeps = ["Dates", "LinearAlgebra"]

    [deps.Compat.extensions]
    CompatLinearAlgebraExt = "LinearAlgebra"

[[deps.CompilerSupportLibraries_jll]]
deps = ["Artifacts", "Libdl"]
uuid = "e66e0078-7015-5450-92f7-15fbd957f2ae"
version = "1.0.5+1"

[[deps.DataStructures]]
deps = ["Compat", "InteractiveUtils", "OrderedCollections"]
git-tree-sha1 = "ac67408d9ddf207de5cfa9a97e114352430f01ed"
uuid = "864edb3b-99cc-5e75-8d2d-829cb0a9cfe8"
version = "0.18.16"

[[deps.Dates]]
deps = ["Printf"]
uuid = "ade2ca70-3891-5945-98fb-dc099432e06a"

[[deps.DiskArrays]]
deps = ["OffsetArrays"]
git-tree-sha1 = "1bfa9de80f35ac63c6c381b2d43c590875896a1f"
uuid = "3c3547ce-8d99-4f5e-a174-61eb10b00ae3"
version = "0.3.22"

[[deps.Downloads]]
deps = ["ArgTools", "FileWatching", "LibCURL", "NetworkOptions"]
uuid = "f43a241f-c20a-4ad4-852c-f6b1247861c6"
version = "1.6.0"

[[deps.FileIO]]
deps = ["Pkg", "Requires", "UUIDs"]
git-tree-sha1 = "c5c28c245101bd59154f649e19b038d15901b5dc"
uuid = "5789e2e9-d7fb-5bc7-8068-2c6fae9b9549"
version = "1.16.2"

[[deps.FileWatching]]
uuid = "7b1f6079-737a-58dc-b8bc-7a2ca5c1b5ee"

[[deps.FixedPointNumbers]]
deps = ["Statistics"]
git-tree-sha1 = "335bfdceacc84c5cdf16aadc768aa5ddfc5383cc"
uuid = "53c48c17-4a7d-5ca2-90c5-79b7896eea93"
version = "0.8.4"

[[deps.HDF5_jll]]
deps = ["Artifacts", "CompilerSupportLibraries_jll", "JLLWrappers", "LazyArtifacts", "LibCURL_jll", "Libdl", "MPICH_jll", "MPIPreferences", "MPItrampoline_jll", "MicrosoftMPI_jll", "OpenMPI_jll", "OpenSSL_jll", "TOML", "Zlib_jll", "libaec_jll"]
git-tree-sha1 = "8156325170d6763b5494c072ac4754214db3e669"
uuid = "0234f1f7-429e-5d53-9886-15a909be8d59"
version = "1.14.3+0"

[[deps.Hyperscript]]
deps = ["Test"]
git-tree-sha1 = "179267cfa5e712760cd43dcae385d7ea90cc25a4"
uuid = "47d2ed2b-36de-50cf-bf87-49c2cf4b8b91"
version = "0.0.5"

[[deps.HypertextLiteral]]
deps = ["Tricks"]
git-tree-sha1 = "7134810b1afce04bbc1045ca1985fbe81ce17653"
uuid = "ac1192a8-f4b3-4bfe-ba22-af5b92cd3ab2"
version = "0.9.5"

[[deps.IOCapture]]
deps = ["Logging", "Random"]
git-tree-sha1 = "8b72179abc660bfab5e28472e019392b97d0985c"
uuid = "b5f81e59-6552-4d32-b1f0-c071b021bf89"
version = "0.2.4"

[[deps.InteractiveUtils]]
deps = ["Markdown"]
uuid = "b77e0a4c-d291-57a0-90e8-8db25a27a240"

[[deps.JLD2]]
deps = ["FileIO", "MacroTools", "Mmap", "OrderedCollections", "Pkg", "PrecompileTools", "Printf", "Reexport", "Requires", "TranscodingStreams", "UUIDs"]
git-tree-sha1 = "7c0008f0b7622c6c0ee5c65cbc667b69f8a65672"
uuid = "033835bb-8acc-5ee8-8aae-3f567f8a3819"
version = "0.4.45"

[[deps.JLLWrappers]]
deps = ["Artifacts", "Preferences"]
git-tree-sha1 = "7e5d6779a1e09a36db2a7b6cff50942a0a7d0fca"
uuid = "692b3bcd-3c85-4b1f-b108-f13ce0eb3210"
version = "1.5.0"

[[deps.JSON]]
deps = ["Dates", "Mmap", "Parsers", "Unicode"]
git-tree-sha1 = "31e996f0a15c7b280ba9f76636b3ff9e2ae58c9a"
uuid = "682c06a0-de6a-54ab-a142-c8b1cf79cde6"
version = "0.21.4"

[[deps.LazyArtifacts]]
deps = ["Artifacts", "Pkg"]
uuid = "4af54fe1-eca0-43a8-85a7-787d91b784e3"

[[deps.LibCURL]]
deps = ["LibCURL_jll", "MozillaCACerts_jll"]
uuid = "b27032c2-a3e7-50c8-80cd-2d36dbcbfd21"
version = "0.6.4"

[[deps.LibCURL_jll]]
deps = ["Artifacts", "LibSSH2_jll", "Libdl", "MbedTLS_jll", "Zlib_jll", "nghttp2_jll"]
uuid = "deac9b47-8bc7-5906-a0fe-35ac56dc84c0"
version = "8.4.0+0"

[[deps.LibGit2]]
deps = ["Base64", "LibGit2_jll", "NetworkOptions", "Printf", "SHA"]
uuid = "76f85450-5226-5b5a-8eaa-529ad045b433"

[[deps.LibGit2_jll]]
deps = ["Artifacts", "LibSSH2_jll", "Libdl", "MbedTLS_jll"]
uuid = "e37daf67-58a4-590a-8e99-b0245dd2ffc5"
version = "1.6.4+0"

[[deps.LibSSH2_jll]]
deps = ["Artifacts", "Libdl", "MbedTLS_jll"]
uuid = "29816b5a-b9ab-546f-933c-edad1886dfa8"
version = "1.11.0+1"

[[deps.Libdl]]
uuid = "8f399da3-3557-5675-b5ff-fb832c97cbdb"

[[deps.Libiconv_jll]]
deps = ["Artifacts", "JLLWrappers", "Libdl"]
git-tree-sha1 = "f9557a255370125b405568f9767d6d195822a175"
uuid = "94ce4f54-9a6c-5748-9c1c-f9c7231a4531"
version = "1.17.0+0"

[[deps.LinearAlgebra]]
deps = ["Libdl", "OpenBLAS_jll", "libblastrampoline_jll"]
uuid = "37e2e46d-f89d-539d-b4ee-838fcccc9c8e"

[[deps.Logging]]
uuid = "56ddb016-857b-54e1-b83d-db4d58db5568"

[[deps.MIMEs]]
git-tree-sha1 = "65f28ad4b594aebe22157d6fac869786a255b7eb"
uuid = "6c6e2e6c-3030-632d-7369-2d6c69616d65"
version = "0.1.4"

[[deps.MPICH_jll]]
deps = ["Artifacts", "CompilerSupportLibraries_jll", "JLLWrappers", "LazyArtifacts", "Libdl", "MPIPreferences", "TOML"]
git-tree-sha1 = "2ee75365ca243c1a39d467e35ffd3d4d32eef11e"
uuid = "7cb0a576-ebde-5e09-9194-50597f1243b4"
version = "4.1.2+1"

[[deps.MPIPreferences]]
deps = ["Libdl", "Preferences"]
git-tree-sha1 = "8f6af051b9e8ec597fa09d8885ed79fd582f33c9"
uuid = "3da0fdf6-3ccc-4f1b-acd9-58baa6c99267"
version = "0.1.10"

[[deps.MPItrampoline_jll]]
deps = ["Artifacts", "CompilerSupportLibraries_jll", "JLLWrappers", "LazyArtifacts", "Libdl", "MPIPreferences", "TOML"]
git-tree-sha1 = "8eeb3c73bbc0ca203d0dc8dad4008350bbe5797b"
uuid = "f1f71cc9-e9ae-5b93-9b94-4fe0e1ad3748"
version = "5.3.1+1"

[[deps.MacroTools]]
deps = ["Markdown", "Random"]
git-tree-sha1 = "2fa9ee3e63fd3a4f7a9a4f4744a52f4856de82df"
uuid = "1914dd2f-81c6-5fcd-8719-6d5c9610ff09"
version = "0.5.13"

[[deps.Markdown]]
deps = ["Base64"]
uuid = "d6f4376e-aef5-505a-96c1-9c027394607a"

[[deps.MbedTLS_jll]]
deps = ["Artifacts", "Libdl"]
uuid = "c8ffd9c3-330d-5841-b78e-0817d7145fa1"
version = "2.28.2+1"

[[deps.MicrosoftMPI_jll]]
deps = ["Artifacts", "JLLWrappers", "Libdl", "Pkg"]
git-tree-sha1 = "b01beb91d20b0d1312a9471a36017b5b339d26de"
uuid = "9237b28f-5490-5468-be7b-bb81f5f5e6cf"
version = "10.1.4+1"

[[deps.Mmap]]
uuid = "a63ad114-7e13-5084-954f-fe012c677804"

[[deps.MozillaCACerts_jll]]
uuid = "14a3606d-f60d-562e-9121-12d972cd8159"
version = "2023.1.10"

[[deps.NCDatasets]]
deps = ["CFTime", "CommonDataModel", "DataStructures", "Dates", "DiskArrays", "NetCDF_jll", "NetworkOptions", "Printf"]
git-tree-sha1 = "4704758e7dd9c843f0a99b18a26aa2d88dd8b8e6"
uuid = "85f8d34a-cbdd-5861-8df4-14fed0d494ab"
version = "0.14.0"

[[deps.NetCDF_jll]]
deps = ["Artifacts", "Bzip2_jll", "HDF5_jll", "JLLWrappers", "LibCURL_jll", "Libdl", "XML2_jll", "Zlib_jll", "Zstd_jll"]
git-tree-sha1 = "10c612c81eaffdd6b7c28a45a554cdd9d2f40ff1"
uuid = "7243133f-43d8-5620-bbf4-c2c921802cf3"
version = "400.902.208+0"

[[deps.NetworkOptions]]
uuid = "ca575930-c2e3-43a9-ace4-1e988b2c1908"
version = "1.2.0"

[[deps.OffsetArrays]]
git-tree-sha1 = "6a731f2b5c03157418a20c12195eb4b74c8f8621"
uuid = "6fe1bfb0-de20-5000-8ca7-80f57d26f881"
version = "1.13.0"

    [deps.OffsetArrays.extensions]
    OffsetArraysAdaptExt = "Adapt"

    [deps.OffsetArrays.weakdeps]
    Adapt = "79e6a3ab-5dfb-504d-930d-738a2a938a0e"

[[deps.OpenBLAS_jll]]
deps = ["Artifacts", "CompilerSupportLibraries_jll", "Libdl"]
uuid = "4536629a-c528-5b80-bd46-f80d51c5b363"
version = "0.3.23+2"

[[deps.OpenMPI_jll]]
deps = ["Artifacts", "CompilerSupportLibraries_jll", "JLLWrappers", "LazyArtifacts", "Libdl", "MPIPreferences", "TOML"]
git-tree-sha1 = "e25c1778a98e34219a00455d6e4384e017ea9762"
uuid = "fe0851c0-eecd-5654-98d4-656369965a5c"
version = "4.1.6+0"

[[deps.OpenSSL_jll]]
deps = ["Artifacts", "JLLWrappers", "Libdl"]
git-tree-sha1 = "cc6e1927ac521b659af340e0ca45828a3ffc748f"
uuid = "458c3c95-2e84-50aa-8efc-19380b2a3a95"
version = "3.0.12+0"

[[deps.OrderedCollections]]
git-tree-sha1 = "dfdf5519f235516220579f949664f1bf44e741c5"
uuid = "bac558e1-5e72-5ebc-8fee-abe8a469f55d"
version = "1.6.3"

[[deps.Parsers]]
deps = ["Dates", "PrecompileTools", "UUIDs"]
git-tree-sha1 = "8489905bcdbcfac64d1daa51ca07c0d8f0283821"
uuid = "69de0a69-1ddd-5017-9359-2bf0b02dc9f0"
version = "2.8.1"

[[deps.Pkg]]
deps = ["Artifacts", "Dates", "Downloads", "FileWatching", "LibGit2", "Libdl", "Logging", "Markdown", "Printf", "REPL", "Random", "SHA", "Serialization", "TOML", "Tar", "UUIDs", "p7zip_jll"]
uuid = "44cfe95a-1eb2-52ea-b672-e2afdf69b78f"
version = "1.10.0"

[[deps.PlutoUI]]
deps = ["AbstractPlutoDingetjes", "Base64", "ColorTypes", "Dates", "FixedPointNumbers", "Hyperscript", "HypertextLiteral", "IOCapture", "InteractiveUtils", "JSON", "Logging", "MIMEs", "Markdown", "Random", "Reexport", "URIs", "UUIDs"]
git-tree-sha1 = "68723afdb616445c6caaef6255067a8339f91325"
uuid = "7f904dfe-b85e-4ff6-b463-dae2292396a8"
version = "0.7.55"

[[deps.PrecompileTools]]
deps = ["Preferences"]
git-tree-sha1 = "03b4c25b43cb84cee5c90aa9b5ea0a78fd848d2f"
uuid = "aea7be01-6a6a-4083-8856-8a6e6704d82a"
version = "1.2.0"

[[deps.Preferences]]
deps = ["TOML"]
git-tree-sha1 = "00805cd429dcb4870060ff49ef443486c262e38e"
uuid = "21216c6a-2e73-6563-6e65-726566657250"
version = "1.4.1"

[[deps.Printf]]
deps = ["Unicode"]
uuid = "de0858da-6303-5e67-8744-51eddeeeb8d7"

[[deps.REPL]]
deps = ["InteractiveUtils", "Markdown", "Sockets", "Unicode"]
uuid = "3fa0cd96-eef1-5676-8a61-b3b8758bbffb"

[[deps.Random]]
deps = ["SHA"]
uuid = "9a3f8284-a2c9-5f02-9a11-845980a1fd5c"

[[deps.Reexport]]
git-tree-sha1 = "45e428421666073eab6f2da5c9d310d99bb12f9b"
uuid = "189a3867-3050-52da-a836-e630ba90ab69"
version = "1.2.2"

[[deps.Requires]]
deps = ["UUIDs"]
git-tree-sha1 = "838a3a4188e2ded87a4f9f184b4b0d78a1e91cb7"
uuid = "ae029012-a4dd-5104-9daa-d747884805df"
version = "1.3.0"

[[deps.SHA]]
uuid = "ea8e919c-243c-51af-8825-aaa63cd721ce"
version = "0.7.0"

[[deps.Serialization]]
uuid = "9e88b42a-f829-5b0c-bbe9-9e923198166b"

[[deps.Sockets]]
uuid = "6462fe0b-24de-5631-8697-dd941f90decc"

[[deps.SparseArrays]]
deps = ["Libdl", "LinearAlgebra", "Random", "Serialization", "SuiteSparse_jll"]
uuid = "2f01184e-e22b-5df5-ae63-d93ebab69eaf"
version = "1.10.0"

[[deps.Statistics]]
deps = ["LinearAlgebra", "SparseArrays"]
uuid = "10745b16-79ce-11e8-11f9-7d13ad32a3b2"
version = "1.10.0"

[[deps.SuiteSparse_jll]]
deps = ["Artifacts", "Libdl", "libblastrampoline_jll"]
uuid = "bea87d4a-7f5b-5778-9afe-8cc45184846c"
version = "7.2.1+1"

[[deps.TOML]]
deps = ["Dates"]
uuid = "fa267f1f-6049-4f14-aa54-33bafae1ed76"
version = "1.0.3"

[[deps.Tar]]
deps = ["ArgTools", "SHA"]
uuid = "a4e569a6-e804-4fa4-b0f3-eef7a1d5b13e"
version = "1.10.0"

[[deps.Test]]
deps = ["InteractiveUtils", "Logging", "Random", "Serialization"]
uuid = "8dfed614-e22c-5e08-85e1-65c5234f0b40"

[[deps.TranscodingStreams]]
git-tree-sha1 = "1fbeaaca45801b4ba17c251dd8603ef24801dd84"
uuid = "3bb67fe8-82b1-5028-8e26-92a6c54297fa"
version = "0.10.2"
weakdeps = ["Random", "Test"]

    [deps.TranscodingStreams.extensions]
    TestExt = ["Test", "Random"]

[[deps.Tricks]]
git-tree-sha1 = "eae1bb484cd63b36999ee58be2de6c178105112f"
uuid = "410a4b4d-49e4-4fbc-ab6d-cb71b17b3775"
version = "0.1.8"

[[deps.URIs]]
git-tree-sha1 = "67db6cc7b3821e19ebe75791a9dd19c9b1188f2b"
uuid = "5c2747f8-b7ea-4ff2-ba2e-563bfd36b1d4"
version = "1.5.1"

[[deps.UUIDs]]
deps = ["Random", "SHA"]
uuid = "cf7118a7-6976-5b1a-9a39-7adc72f591a4"

[[deps.Unicode]]
uuid = "4ec0a83e-493e-50e2-b9ac-8f72acf5a8f5"

[[deps.XML2_jll]]
deps = ["Artifacts", "JLLWrappers", "Libdl", "Libiconv_jll", "Zlib_jll"]
git-tree-sha1 = "801cbe47eae69adc50f36c3caec4758d2650741b"
uuid = "02c8fc9c-b97f-50b9-bbe4-9be30ff0a78a"
version = "2.12.2+0"

[[deps.Zlib_jll]]
deps = ["Libdl"]
uuid = "83775a58-1f1d-513f-b197-d71354ab007a"
version = "1.2.13+1"

[[deps.Zstd_jll]]
deps = ["Artifacts", "JLLWrappers", "Libdl"]
git-tree-sha1 = "49ce682769cd5de6c72dcf1b94ed7790cd08974c"
uuid = "3161d3a3-bdf6-5164-811a-617609db77b4"
version = "1.5.5+0"

[[deps.libaec_jll]]
deps = ["Artifacts", "JLLWrappers", "Libdl"]
git-tree-sha1 = "eddd19a8dea6b139ea97bdc8a0e2667d4b661720"
uuid = "477f73a3-ac25-53e9-8cc3-50b2fa2566f0"
version = "1.0.6+1"

[[deps.libblastrampoline_jll]]
deps = ["Artifacts", "Libdl"]
uuid = "8e850b90-86db-534c-a0d3-1478176c7d93"
version = "5.8.0+1"

[[deps.nghttp2_jll]]
deps = ["Artifacts", "Libdl"]
uuid = "8e850ede-7688-5339-a07c-302acd2aaf8d"
version = "1.52.0+1"

[[deps.p7zip_jll]]
deps = ["Artifacts", "Libdl"]
uuid = "3f19e933-33d8-53b3-aaab-bd5110c3b7a0"
version = "17.4.0+2"
"""

# ╔═╡ Cell order:
# ╠═d5739304-8a32-4b10-a224-ea178f8f8313
# ╠═3c01262b-c4b6-4f44-9c19-f2631c453922
# ╠═f43b6400-b516-11ee-0ffd-ad6fe8cbd4d9
# ╠═cb4a084a-3d03-4dff-91ad-dcab7366fbb6
# ╠═71818093-b3f6-4a87-97f7-bdaca574ebab
# ╠═7a327f9b-5aff-4067-8def-0f7bed5863f9
# ╠═d48f2f00-586d-4237-8cb0-8becfae45de2
# ╠═54af094d-22b1-4a46-ac85-f01ff258ab0f
# ╠═548c49a4-68e4-4b31-b22d-fb2cbe38964f
# ╠═48a23f0d-c1fe-4a90-91b5-ca7dd7bd72a4
# ╠═a2458f44-bedc-48ef-86c3-2367f82c3933
# ╠═f1b8e784-35f1-4169-8abb-7377f27fb97e
# ╠═6d3e76f4-ba4f-4934-9750-adb837169564
# ╠═1d0414a2-321d-4403-a06d-30d515edf3dc
# ╠═9027800c-593b-4ff4-8150-78fc9ebb3c5b
# ╠═466261cc-4e50-4bc4-9088-ebba1698aa78
# ╠═d44d080b-a3fa-40f7-9392-b4426eb88691
# ╠═53c2c958-d129-4377-b75a-2b3139069721
# ╠═647d91b3-e0c7-416e-a81c-31fc1f819e94
# ╠═221de565-8042-49e7-b80a-5a5dc7865e45
# ╠═222a7a75-a5a5-491c-a4fa-b76b39ec6979
# ╠═a2139158-3347-4aa5-8039-10cbf945352a
# ╠═e5fcffc8-4171-40df-8d52-3be6e3bc354f
# ╟─00000000-0000-0000-0000-000000000001
# ╟─00000000-0000-0000-0000-000000000002

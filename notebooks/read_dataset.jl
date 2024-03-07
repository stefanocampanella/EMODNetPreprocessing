### A Pluto.jl notebook ###
# v0.19.39

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

# ╔═╡ 2b8051de-f47f-4429-b0cd-7870a08767d9
begin
	using Pkg

	Pkg.activate(joinpath(@__DIR__, ".."))
end

# ╔═╡ d5739304-8a32-4b10-a224-ea178f8f8313
using PlutoUI

# ╔═╡ 3c01262b-c4b6-4f44-9c19-f2631c453922
using TOML

# ╔═╡ f43b6400-b516-11ee-0ffd-ad6fe8cbd4d9
using NCDatasets

# ╔═╡ cb4a084a-3d03-4dff-91ad-dcab7366fbb6
using Dates

# ╔═╡ 8746f94f-4766-4e59-855c-952791f1083d
using DataFrames

# ╔═╡ 1652f297-a8d8-4d03-9dfa-295453746f57
using GeoMakie, CairoMakie

# ╔═╡ e2c58f06-3be5-4615-a12f-52f72c9fd7b7
using JLD2

# ╔═╡ fbca368e-1d78-4943-bfdd-e0f21aa2a37f
using PythonCall

# ╔═╡ bd94f428-f9b5-4fdf-b35a-8f55ef245083
using CSV

# ╔═╡ 01128af1-5c6a-4be7-82f3-b45709ecfec1
md"""
# Extract timeseries/profile data from EMODnet NetCDF

## Preliminary operations

As a preliminary operation, activate current environment and load the needed packages. Afterwards load the configuration file, and read the EMODnet input file.
"""

# ╔═╡ 71818093-b3f6-4a87-97f7-bdaca574ebab
md"""Settings file: $(@bind settingsfilepath TextField(default="../configs/nutrients.toml"))"""

# ╔═╡ 7a327f9b-5aff-4067-8def-0f7bed5863f9
settings = TOML.parsefile(settingsfilepath)

# ╔═╡ 443a99a3-e3a5-412e-8d3c-371cc5126f22
md"""Input series type: $(@bind seriestype Select(["timeseries", "profile"], default="profile"))"""

# ╔═╡ 5555d76d-0377-472a-8cfe-0df2ebb8093f
#Input NetCDF
@bind ncinputfilepath TextField(default="../data/emodnet-chem_2023_$(seriestype).nc")

# ╔═╡ d48f2f00-586d-4237-8cb0-8becfae45de2
emodnet2022 = NCDataset(ncinputfilepath, "r")

# ╔═╡ a80f194d-aa16-4713-a149-d4672ddfc7f7
md"""

## Reading time, location and id variables

The following variables are defined for each station, they will be mapped onto each entry of the dataframe, based on the value of the station index.

They are not listed in the configuration file, since they are always needed.
"""

# ╔═╡ 54af094d-22b1-4a46-ac85-f01ff258ab0f
"""
Read a vector of characters, strip the '\0's and return an eventually empty string
"""
function stringfromvec(cs)
	m = match(r"[^\0]+", String(cs))
	if isnothing(m)
		""
	else
		m.match
	end
end

# ╔═╡ 00f05bf4-b449-4ea0-b877-830ed661db52
instrument_info = map(stringfromvec, eachcol(emodnet2022["Instrument_Info"][:, :]))

# ╔═╡ 3d2fcbe6-43bb-45cc-a786-63c480a55d9f
instrument_type = map(s -> contains(s, r"(?i)DOXYWITX|DOKGWITX") ? "nut" : "probe" , instrument_info)

# ╔═╡ 827e8bcf-e77b-456c-9257-dad0b4a74f6d
cruise_id = map(stringfromvec, eachcol(emodnet2022["cruise_id"][:, :]))

# ╔═╡ bfbd6048-8820-499a-85eb-14127a1dd3cd
station_id = map(stringfromvec, eachcol(emodnet2022["station_id"][:, :]))

# ╔═╡ a2458f44-bedc-48ef-86c3-2367f82c3933
platform_type = map(stringfromvec, eachcol(emodnet2022["Platform_type"][:, :]))

# ╔═╡ 6d3e76f4-ba4f-4934-9750-adb837169564
latitude = emodnet2022["latitude"][:]

# ╔═╡ 1d0414a2-321d-4403-a06d-30d515edf3dc
longitude = emodnet2022["longitude"][:]

# ╔═╡ 6e5f6c46-53c9-4cb6-aa8c-925087595c95
md"""
Depths and dates are intertwined matrices. Each column represent all the data points collected by a station. They can have repeated entries, one for each combination of depth and datetime.

The datetime is stored in decimal years since the 1st of January of Year zero.

Since without any of these two variables a data point is invalid, `mask` stores the locations of valid data points. Subsequent filters are applied afterwards.
"""

# ╔═╡ 9027800c-593b-4ff4-8150-78fc9ebb3c5b
depths = emodnet2022["Depth"][:, :]

# ╔═╡ 32580d32-6783-4a0e-a11e-2e6232d3b4e6
"""
	decimalyear2datetime(t)

Converts a float representing a decimal year to a datetime. It computes the number of milliseconds in the interval between the 1st of January of the current year and the same date of the next year. Then it scales this interval by the decimal part of `t`, and adds this quantity to the current year.
"""
function decimalyear2datetime(t::AbstractFloat)
		next_year = DateTime(ceil(Int, t))
		current_year = DateTime(floor(Int, t))
		milliseconds = next_year - current_year
		current_year + Millisecond(round(Int, (t - floor(t)) * milliseconds.value))
end

# ╔═╡ 95bcdd24-b66a-4514-9042-1b1fd069963f
decimalyear2datetime(t::Missing) = missing

# ╔═╡ 44771df6-5826-4722-b800-b2f936dcdd6a
if seriestype == "timeseries"
	datetime = map(decimalyear2datetime, emodnet2022["time_ISO8601"][:, :])
elseif seriestype == "profile"
	datetime = emodnet2022["date_time"][:]
end

# ╔═╡ 4cd34ff6-b13e-42d5-a7c0-3acc7a8ad9a7
if seriestype == "timeseries"
	mask = map(!ismissing, depths) .& map(!ismissing, datetime)
elseif seriestype == "profile"
	mask = map(!ismissing, depths)
end

# ╔═╡ d44d080b-a3fa-40f7-9392-b4426eb88691
# Number of valid data points
npoints = count(mask)

# ╔═╡ 53c2c958-d129-4377-b75a-2b3139069721
# Mapping between linear index of depths, datetimes and mask, and station index
station = map(x -> x[2], eachindex(IndexCartesian(), mask))[mask]

# ╔═╡ 5534d741-be25-47d6-88d9-844fd6ce8ee0
md"""
## Preparing the raw (all including) DataFrame

In the following the list of nutrient variables is read from the settings file, and a dataframe containing the previously defined variables along with the new ones is defined.
"""

# ╔═╡ c1fd45aa-8e69-45f2-8468-87e877dd00e8
begin
	df = Dict{String, Vector}()
	
	for sym in [:longitude, :latitude, :cruise_id, :instrument_type, :platform_type, :station_id]
		@info "Processing $(string(sym))"
		df[string(sym)] = @eval $sym[station]
	end

	@info "Processing station"
	df["station_ncref"] = station
	
	@info "Processing depth"
	df["depth"] = depths[mask]
	
	@info "Processing datetime"
	if seriestype == "timeseries"
		df["datetime"] = datetime[mask]
	elseif seriestype == "profile"
		df["datetime"] = datetime[station]
	end

	for var in settings["variables"]
		output_name = var["output"]
		@info "Processing $output_name"
		input_name = get(var, "input", missing)
		if !ismissing(input_name) 
			if haskey(emodnet2022, input_name)
				df[output_name] = emodnet2022[input_name][mask]
			else
				df[output_name] = Vector{Union{Missing, Float32}}(missing, npoints)
			end
			if haskey(emodnet2022, input_name * "_qc")
				df[output_name * "_qc"] = emodnet2022[input_name * "_qc"][mask]
			else
				df[output_name * "_qc"] = Vector{Union{Missing, Float32}}(missing, npoints)
			end
		end
	end
	df = DataFrame(df)
end

# ╔═╡ c8cc2a5d-1ae5-4d77-a992-29b14f6551b5
let
	fig = Figure()
	ax = Axis(fig[1, 1], title="Time distribution of data points")
	years = map(year, df.datetime)
	hist!(ax, years)
	fig
end

# ╔═╡ 92ef98bc-fa31-4030-b631-a435bd8817c8
let
	fig = Figure()
	ax = GeoAxis(
		fig[1, 1], 
		limits=(extrema(df.longitude)..., extrema(df.latitude)...), 
		dest="+proj=merc")
	scatter!(ax, df.longitude, df.latitude, markersize=6)
	lines!(ax, GeoMakie.coastlines())
	fig
end

# ╔═╡ d600917e-0ef4-4222-937c-207a0be887db
md"""

## Define filter functions and filter the raw DataFrame

"""

# ╔═╡ 737eaacd-7693-4d6d-9974-29080326c6b2
withinrange(x, (a, b)) = a < x < b 

# ╔═╡ 23dacf08-42e2-4f87-8647-3c7738fa988c
withinrange(range) = x -> withinrange(x, range)

# ╔═╡ 0b5d50d6-3d46-444e-8ce5-12196fbf51f3
coordinatesfilters = let
	datelims = get(settings["filters"], "datetime", [DateTime(0), today()])
	latlims = get(settings["filters"], "latitude", [-90, 90])
	lonlims = get(settings["filters"], "longitude", [-180, 180])
	depthlims = get(settings["filters"], "depth", [-Inf, 0])

	filters = Pair{Symbol, Function}[
		:datetime => withinrange(datelims),
		:longitude => withinrange(lonlims),
		:latitude => withinrange(latlims),
		:depth => withinrange(depthlims)]

	filters
end

# ╔═╡ f7cbc66b-9156-4dd4-b250-9cd3c25554d7
rangefilters = let
	filters = Pair{Symbol, Function}[]
	
	# Add filters on variable ranges
	for var in settings["variables"]
		name = get(var, "output", missing)
		if !ismissing(name)
			range = get(var, "range", missing)
			if !ismissing(range)
				push!(filters, Symbol(name) => x -> ismissing(x) || withinrange(x, range))
			end
		end
	end

	filters
end

# ╔═╡ 5d3f879a-1905-48be-8dad-439ef7b84208
qcfilters = let
	filters = Pair{Symbol, Function}[]

	# Add filters on variable ranges
	for var in settings["variables"]
		name = get(var, "output", missing)
		if !ismissing(name)
			allowed_qcs = get(var, "qc", missing)
			if !ismissing(range)
				push!(filters, Symbol(name * "_qc") => x -> ismissing(x) || x in allowed_qcs)
			end
		end
	end

	filters
end

# ╔═╡ 48a23f0d-c1fe-4a90-91b5-ca7dd7bd72a4
allowed_platforms = settings["filters"]["platforms"]

# ╔═╡ 2049f4c9-fbd2-421c-bdc8-b31a15249342
platformfilter :: Pair{Symbol, Function} = :platform_type => in(allowed_platforms)

# ╔═╡ 6b464a87-86c4-49e4-9cfb-a318e954651e
cruisefilter :: Pair{Symbol, Function} = :cruise_id => !contains(r"(?i)argo|glider|float|wmo")

# ╔═╡ 6572d314-3d81-4b20-a09c-4cf18333ed04
function hifreqfilter(nhours)
	timeintervals = map((((x, y),) -> y - x) ∘ extrema ∘ unique ∘ skipmissing, eachcol(datetime))
	numsamples = map(length ∘ unique ∘ skipmissing, eachcol(datetime))

	valid_stations = findall(@. timeintervals >= (numsamples - 1) * Millisecond(Hour(nhours)))

	:station_ncref => in(valid_stations)
end

# ╔═╡ 9de3e161-c8e6-49a9-82ad-c2ee0bb20ddc
begin
	allfilters = [coordinatesfilters; platformfilter; qcfilters; rangefilters; cruisefilter]
	if seriestype == "timeseries"
		push!(allfilters, hifreqfilter(24))
	end
end

# ╔═╡ 7b1a7621-42d8-4690-b129-301bb536022e
function applyfilters!(filters, df)
	for f in allfilters
		filter!(f, df)
	end
end

# ╔═╡ 678eba31-4d2a-45cd-b092-d87eaf06d07c
begin
	data = deepcopy(df)
	applyfilters!(allfilters, data)
	data
end

# ╔═╡ 795a8001-27b0-445d-bc5c-7f3a98e37818
let
	fig = Figure()
	ax = Axis(fig[1, 1], title="Time distribution of data points")
	years = map(year, data.datetime)
	hist!(ax, years)
	fig
end

# ╔═╡ 528ef529-ac3b-4f4a-93c7-5777e39d717a
let
	fig = Figure()
	ax = GeoAxis(
		fig[1, 1], 
		limits=(extrema(data.longitude)..., extrema(data.latitude)...), 
		dest="+proj=merc")
	scatter!(ax, data.longitude, data.latitude, markersize=6)
	lines!(ax, GeoMakie.coastlines())
	fig
end

# ╔═╡ c34d3b62-0896-4e3a-95cf-648413a855b6
function getmask(filter::Pair{Symbol, Function}, df)
	s, f = filter
	map(f, getproperty(df, s))
end

# ╔═╡ 38c31bad-b757-46ce-a9c9-2fb01804c5be
function getmask(filters::AbstractVector{Pair{Symbol, Function}}, df)
	mask = trues(nrow(df))
	for filter in filters
		mask .&= getmask(filter, df)
	end
	mask
end

# ╔═╡ 8d2bd40b-9cd0-43d0-9d17-228dcf373147
bad_data = let
	# platform & qc & ( ~range | cruise)
	mask = @. $getmask(platformfilter, df) & $getmask(qcfilters, df) & (! $getmask(rangefilters, df) | ! $getmask(cruisefilter, df))
	df[mask, :]
end	

# ╔═╡ b2b8c252-742e-40ef-b7bc-713aaa933199
#Bad data output file
@bind bad_datafilepath TextField(default="../data/bad_$seriestype.csv")

# ╔═╡ e9f4aec9-131a-42d4-87aa-31fab3aa6942
CSV.write(bad_datafilepath, bad_data)

# ╔═╡ 1a16d460-1653-44d1-a675-b712774d850d
#JLD2 output file
@bind jld2filepath TextField(default="../data/nutrients_$seriestype.jld2")

# ╔═╡ 7e2fb05b-9a38-40c9-9997-84096dd72124
jldsave(jld2filepath, true; data)

# ╔═╡ cef81823-6242-42b8-95fe-c6b930e4e36e
data_nomissing = coalesce.(data, NaN)

# ╔═╡ b718ca09-9e7f-4b4e-9dc2-a13ae59750e6
data_pydf = pytable(data_nomissing)

# ╔═╡ 2abba5d4-74cf-4517-933d-9d1a0d700688
#Parquet output file
@bind parquetfilepath TextField(default="../data/nutrients_$seriestype.parquet")

# ╔═╡ f7dd4a02-1292-4c1a-87ae-e85e93b12bae
data_pydf.to_parquet(parquetfilepath)

# ╔═╡ 221de565-8042-49e7-b80a-5a5dc7865e45
# ╠═╡ disabled = true
#=╠═╡
function matfromstrings(strings)
	len = maximum(length, strings)
	strings = map(s -> rpad(s, len), strings)
	reduce(hcat, map(collect, strings))
end
  ╠═╡ =#

# ╔═╡ 222a7a75-a5a5-491c-a4fa-b76b39ec6979
#=╠═╡
Dataset(settings["io"]["output"], "c") do ds
	varnames = [var["output"] for var in settings["variables"]]
	unitnames = [haskey(var, "unit") ? var["unit"] : var["output"] for var in settings["variables"]]
	data_matrix = reduce(hcat, (data[name] for name in varnames))
	
	defVar(ds, "Cruises", matfromstrings(cruisenames), ("lendataset", "nCruises"))
	defVar(ds, "DATA", data_matrix, ("nData", "nVars"), fillvalue=1.0e20)
	defVar(ds, "UNITS", matfromstrings(unitnames), ("lenunits", "nVars"))
	defVar(ds, "VARIABLES", matfromstrings(varnames), ("lenvars", "nVars"))
end
  ╠═╡ =#

# ╔═╡ Cell order:
# ╟─01128af1-5c6a-4be7-82f3-b45709ecfec1
# ╠═2b8051de-f47f-4429-b0cd-7870a08767d9
# ╠═d5739304-8a32-4b10-a224-ea178f8f8313
# ╠═3c01262b-c4b6-4f44-9c19-f2631c453922
# ╠═f43b6400-b516-11ee-0ffd-ad6fe8cbd4d9
# ╠═cb4a084a-3d03-4dff-91ad-dcab7366fbb6
# ╠═8746f94f-4766-4e59-855c-952791f1083d
# ╠═1652f297-a8d8-4d03-9dfa-295453746f57
# ╠═e2c58f06-3be5-4615-a12f-52f72c9fd7b7
# ╠═fbca368e-1d78-4943-bfdd-e0f21aa2a37f
# ╠═bd94f428-f9b5-4fdf-b35a-8f55ef245083
# ╟─71818093-b3f6-4a87-97f7-bdaca574ebab
# ╠═7a327f9b-5aff-4067-8def-0f7bed5863f9
# ╟─443a99a3-e3a5-412e-8d3c-371cc5126f22
# ╠═5555d76d-0377-472a-8cfe-0df2ebb8093f
# ╠═d48f2f00-586d-4237-8cb0-8becfae45de2
# ╟─a80f194d-aa16-4713-a149-d4672ddfc7f7
# ╠═54af094d-22b1-4a46-ac85-f01ff258ab0f
# ╠═00f05bf4-b449-4ea0-b877-830ed661db52
# ╠═3d2fcbe6-43bb-45cc-a786-63c480a55d9f
# ╠═827e8bcf-e77b-456c-9257-dad0b4a74f6d
# ╠═bfbd6048-8820-499a-85eb-14127a1dd3cd
# ╠═a2458f44-bedc-48ef-86c3-2367f82c3933
# ╠═6d3e76f4-ba4f-4934-9750-adb837169564
# ╠═1d0414a2-321d-4403-a06d-30d515edf3dc
# ╟─6e5f6c46-53c9-4cb6-aa8c-925087595c95
# ╠═9027800c-593b-4ff4-8150-78fc9ebb3c5b
# ╟─32580d32-6783-4a0e-a11e-2e6232d3b4e6
# ╠═95bcdd24-b66a-4514-9042-1b1fd069963f
# ╠═44771df6-5826-4722-b800-b2f936dcdd6a
# ╠═4cd34ff6-b13e-42d5-a7c0-3acc7a8ad9a7
# ╠═d44d080b-a3fa-40f7-9392-b4426eb88691
# ╠═53c2c958-d129-4377-b75a-2b3139069721
# ╟─5534d741-be25-47d6-88d9-844fd6ce8ee0
# ╠═c1fd45aa-8e69-45f2-8468-87e877dd00e8
# ╠═c8cc2a5d-1ae5-4d77-a992-29b14f6551b5
# ╠═92ef98bc-fa31-4030-b631-a435bd8817c8
# ╟─d600917e-0ef4-4222-937c-207a0be887db
# ╠═737eaacd-7693-4d6d-9974-29080326c6b2
# ╠═23dacf08-42e2-4f87-8647-3c7738fa988c
# ╠═0b5d50d6-3d46-444e-8ce5-12196fbf51f3
# ╠═f7cbc66b-9156-4dd4-b250-9cd3c25554d7
# ╠═5d3f879a-1905-48be-8dad-439ef7b84208
# ╠═48a23f0d-c1fe-4a90-91b5-ca7dd7bd72a4
# ╠═2049f4c9-fbd2-421c-bdc8-b31a15249342
# ╠═6b464a87-86c4-49e4-9cfb-a318e954651e
# ╠═6572d314-3d81-4b20-a09c-4cf18333ed04
# ╠═9de3e161-c8e6-49a9-82ad-c2ee0bb20ddc
# ╠═7b1a7621-42d8-4690-b129-301bb536022e
# ╠═678eba31-4d2a-45cd-b092-d87eaf06d07c
# ╠═795a8001-27b0-445d-bc5c-7f3a98e37818
# ╠═528ef529-ac3b-4f4a-93c7-5777e39d717a
# ╠═c34d3b62-0896-4e3a-95cf-648413a855b6
# ╠═38c31bad-b757-46ce-a9c9-2fb01804c5be
# ╠═8d2bd40b-9cd0-43d0-9d17-228dcf373147
# ╠═b2b8c252-742e-40ef-b7bc-713aaa933199
# ╠═e9f4aec9-131a-42d4-87aa-31fab3aa6942
# ╠═1a16d460-1653-44d1-a675-b712774d850d
# ╠═7e2fb05b-9a38-40c9-9997-84096dd72124
# ╠═cef81823-6242-42b8-95fe-c6b930e4e36e
# ╠═b718ca09-9e7f-4b4e-9dc2-a13ae59750e6
# ╠═2abba5d4-74cf-4517-933d-9d1a0d700688
# ╠═f7dd4a02-1292-4c1a-87ae-e85e93b12bae
# ╠═221de565-8042-49e7-b80a-5a5dc7865e45
# ╠═222a7a75-a5a5-491c-a4fa-b76b39ec6979

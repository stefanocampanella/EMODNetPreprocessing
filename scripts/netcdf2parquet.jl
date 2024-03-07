"""
"""

using NCDatasets
using DataFrames
using Parquet2

using TOML
using Dates

"""
    stringfromvec(cs)

    Read a vector of characters `cs`, strip the '\0's and return an eventually empty string
"""
function stringfromvec(cs)
    m = match(r"[^\0]+", String(cs))
    if isnothing(m)
        ""
    else
        m.match
    end
end

"""
    applyfilters!(filters, df)

    Applies the filters in `filters` to the DataFrame `df` in place.
"""
function applyfilters!(filters, df)
    for f in filters
        filter!(f, df)
    end
end

"""
    withinrange(x, range)

    Returns `true` if `x` is within the range `range`, `false` otherwise.
"""
withinrange(x, (a, b)) = a < x < b 
withinrange(range) = x -> withinrange(x, range)

"""
    getmask(filters, df)

    Returns a boolean mask for the DataFrame `df` according to filters in `filter`.
"""
function getmask(filter::Pair{Symbol, Function}, df)
    s, f = filter
    map(f, getproperty(df, s))
end

function getmask(filters::AbstractVector{Pair{Symbol, Function}}, df)
    mask = trues(nrow(df))
    for filter in filters
        mask .&= getmask(filter, df)
    end
    mask
end

"""
    hiresfilter(var, delta)

    Returns a filter that drops stations with average difference between points less than `delta`.
"""
function hiresfilter(var, delta)
    ranges = map((((x, y),) -> y - x) ∘ extrema ∘ skipmissing, eachcol(var))
    # In case of timeseries, there are duplicate timestamps, so we need to count unique timestamps
    numsamples = map(length ∘ unique ∘ skipmissing, eachcol(var))

    valid_stations = findall(@. ranges >= (numsamples - 1) * delta)

    :station_ncref => in(valid_stations)
end

# Read command line arguments
for (sym, arg) in zip([:seriestype, :settingspath, :ncinputpath, :parquetoutputpath], ARGS)
    @eval $sym = $arg
end

# Read settings file and the input NetCDF file
@info "Reading settings at $settingspath and input file at $ncinputpath"
settings = TOML.parsefile(settingspath)
emodnet2022 = NCDataset(ncinputpath, "r")

# Extract coordinates and measurements metadata from the NetCDF file
instrument_info = map(stringfromvec, eachcol(emodnet2022["Instrument_Info"][:, :]))
instrument_type = map(s -> contains(s, r"(?i)DOXYWITX|DOKGWITX") ? "nut" : "probe" , instrument_info)
cruise_id = map(stringfromvec, eachcol(emodnet2022["cruise_id"][:, :]))
station_id = map(stringfromvec, eachcol(emodnet2022["station_id"][:, :]))
platform_type = map(stringfromvec, eachcol(emodnet2022["Platform_type"][:, :]))
latitude = emodnet2022["latitude"][:]
longitude = emodnet2022["longitude"][:]
depths = emodnet2022["Depth"][:, :]

# datetime is defined differently for timeseries and profile data
if seriestype == "timeseries"
    datetime = emodnet2022["time_ISO8601"][:, :]
elseif seriestype == "profile"
    datetime = emodnet2022["date_time"][:]
end

# Mask for valid data points, i.e. those that are not missing
if seriestype == "timeseries"
    mask = map(!ismissing, depths) .& map(!ismissing, datetime)
elseif seriestype == "profile"
    mask = map(!ismissing, depths)
end

# Number of valid data points
npoints = count(mask)

# Mapping between linear index of depths, datetimes and mask, and station index
station = map(x -> x[2], eachindex(IndexCartesian(), mask))[mask]

# Convert coordinates and metadata into columns of a DataFrame, then read and add to it the variables listed in settings 
begin
    df = DataFrame()
    
    for sym in [:longitude, :latitude, :cruise_id, :instrument_type, :platform_type, :station_id]
        @info "Processing $(string(sym))"
        df[!, sym] = @eval $sym[station]
    end

    @info "Processing station"
    df[!, "station_ncref"] = station
    
    @info "Processing depth"
    df[!, "depth"] = depths[mask]
    
    @info "Processing datetime"
    if seriestype == "timeseries"
        df[!, "datetime"] = datetime[mask]
    elseif seriestype == "profile"
        df[!, "datetime"] = datetime[station]
    end

    for var in settings["variables"]
        output_name = var["output"]
        @info "Processing $output_name"
        input_name = get(var, "input", missing)
        if !ismissing(input_name) 
            if haskey(emodnet2022, input_name)
                df[!, output_name] = emodnet2022[input_name][mask]
            else
                df[!, output_name] = Vector{Union{Missing, Float32}}(missing, npoints)
            end
            if haskey(emodnet2022, input_name * "_qc")
                df[!, output_name * "_qc"] = emodnet2022[input_name * "_qc"][mask]
            else
                df[!, output_name * "_qc"] = Vector{Union{Missing, Float32}}(missing, npoints)
            end
        end
    end
end

"""
    coordinatesfilters

    Contains a list of filters for the coordinates (space and time) range in the settings file.
"""
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

"""
    rangefilters

    Contains a list of filters for the range of the variables in the settings file.
"""
rangefilters = let
    filters = Pair{Symbol, Function}[]
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

"""
    qcfilters

    Contains a list of filters for the quality control flags of the variables in the settings file.
"""
qcfilters = let
    filters = Pair{Symbol, Function}[]
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

allowed_platforms = settings["filters"]["platforms"]
"""
    platformfilter

    Filter for the allowed platform types listed in the in the settings file.
"""
platformfilter :: Pair{Symbol, Function} = :platform_type => in(allowed_platforms)

cruisefilter :: Pair{Symbol, Function} = :cruise_id => !contains(r"argo|glider|float|wmo"i)

# Apply all filters and write the resulting DataFrame to a parquet file
let
    allfilters = [coordinatesfilters; platformfilter; qcfilters; rangefilters; cruisefilter]
    if seriestype == "timeseries"
        push!(allfilters, hiresfilter(datetime, Millisecond(Day(1)))) # drop stations with average time between points less than 1 day 
    else
        push!(allfilters, hiresfilter(depth, 1.0)) # drop stations with average depth difference less than 1 meter
    end
    applyfilters!(allfilters, df)
    @info "Writing data to $parquetoutputpath"
    Parquet2.writefile(parquetoutputpath, df)
end

# Write suspicious data points to a separate parquet file
let
    # platform & qc & ( ~range | cruise)
    mask = @. $getmask(platformfilter, df) & $getmask(qcfilters, df) & (! $getmask(rangefilters, df) | ! $getmask(cruisefilter, df))
    filename, extension = splitext(parquetoutputpath)
    @info "Writing suspicious data to $(filename * "_suspicious" * extension)"
    Parquet2.writefile(filename * "_suspicious" * extension, df[mask, :])
end	
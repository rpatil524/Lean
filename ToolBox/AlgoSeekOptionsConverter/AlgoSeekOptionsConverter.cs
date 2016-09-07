/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

using System;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using QuantConnect.Logging;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Collections.Generic;
<<<<<<< HEAD
using System.Text;
using QuantConnect.Data;
using QuantConnect.Data.Market;
=======
using QuantConnect.Data;
>>>>>>> 4fdbe024fb90617e2e30fe079b04eca3fd72e4a7
using QuantConnect.Lean.Engine.DataFeeds.Enumerators;
using QuantConnect.Util;

namespace QuantConnect.ToolBox.AlgoSeekOptionsConverter
{
    /// <summary>
    /// Process a directory of algoseek option files into separate resolutions.
    /// </summary>
    public class AlgoSeekOptionsConverter
    {
        private string _source;
        private string _destination;
<<<<<<< HEAD
        private DateTime _referenceDate;
        private Resolution[] _resolutions;
        private Dictionary<Symbol, List<AlgoSeekOptionsProcessor>> _processors;

=======
        private long _flushInterval;
        private DateTime _referenceDate;
        private Resolution[] _resolutions;
        private Dictionary<Symbol, List<AlgoSeekOptionsProcessor>> _processors;
        private bool _inMemoryProcessing;
        
>>>>>>> 4fdbe024fb90617e2e30fe079b04eca3fd72e4a7

        /// <summary>
        /// Create a new instance of the AlgoSeekOptions Converter. Parse a single input directory into an output.
        /// </summary>
        /// <param name="referenceDate">Datetime to be added to the milliseconds since midnight. Algoseek data is stored in channel files (XX.bz2) and in a source directory</param>
        /// <param name="source">Source directory of the .bz algoseek files</param>
        /// <param name="destination">Data directory of LEAN</param>
<<<<<<< HEAD
        public AlgoSeekOptionsConverter(DateTime referenceDate, string source, string destination)
=======
        /// <param name="inMemoryProcessing"></param>
        public AlgoSeekOptionsConverter(DateTime referenceDate, string source, string destination, bool inMemoryProcessing = true)
>>>>>>> 4fdbe024fb90617e2e30fe079b04eca3fd72e4a7
        {
            _source = source;
            _referenceDate = referenceDate;
            _destination = destination;
<<<<<<< HEAD
            _processors = new Dictionary<Symbol, List<AlgoSeekOptionsProcessor>>();
        }

        /// <summary>
        /// Give the reference date and source directory, convert the algoseek options data into n-resolutions LEAN format.
        /// </summary>
        public void Convert(Resolution resolution)
        {
            //Tidy up any existing files:
            Log.Trace("AlgoSeekOptionsConverter.Convert(): Cleaning input..");
            //Directory.EnumerateFiles(_source, "*.csv").ToList().ForEach(file => { File.Delete(file); });

            //Get the list of all the files, then for each file open a separate streamer.
            var files = Directory.EnumerateFiles(_source, "*.bz2");
            Log.Trace("AlgoSeekOptionsConverter.Convert(): Loading {0} AlgoSeekOptionsReader for {1} ", files.Count(), _referenceDate);

            //Initialize parameters
            var totalLinesProcessed = 0L;
            var frontier = DateTime.MinValue;
            var estimatedEndTime = _referenceDate.AddHours(16);
            var zipper = OS.IsWindows ? "C:/Program Files/7-Zip/7z.exe" : "7z";

            //Extract each file massively in parallel.
            Parallel.ForEach(files, file =>
            {
                if (File.Exists(file.Replace(".bz2", ""))) return;
                Log.Trace("AlgoSeekOptionsConverter.Convert(): Extracting " + file);
                var psi = new ProcessStartInfo(zipper, " e " + file + " -o" + _source)
                {
                    CreateNoWindow = true,
                    WindowStyle = ProcessWindowStyle.Hidden
                };
                var process = Process.Start(psi);
                process.WaitForExit();
                if (process.ExitCode > 0)
                {
                    throw new Exception("7Zip Exited Unsuccessfully: " + file);
                }
            });

            //Fetch the new CSV files:
            files = Directory.EnumerateFiles(_source, "*.csv");
            if (!files.Any()) throw new Exception("No csv files found");

            // Create multithreaded readers; start them in threads and store the ticks in queues
            var readers = files.Select(file => new AlgoSeekOptionsReader(file, _referenceDate));
            var synchronizer = new SynchronizingEnumerator(readers);
=======
            _flushInterval = 1000000;
            _inMemoryProcessing = inMemoryProcessing;
            _processors = new Dictionary<Symbol, List<AlgoSeekOptionsProcessor>>();
        }
        
        /// <summary>
        /// Give the reference date and source directory, convert the algoseek options data into n-resolutions LEAN format.
        /// </summary>
        public void Convert(params Resolution[] resolutions)
        {
            _resolutions = resolutions;

            //Get the list of all the files, then for each file open a separate streamer.
            var files = Directory.EnumerateFiles(_source).OrderByDescending(x => new FileInfo(x).Length).ToList();
            Log.Trace("AlgoSeekOptionsConverter.Convert(): Loading {0} AlgoSeekOptionsReader for {1}: In Memory Mode: {2}", files.Count(), _referenceDate, _inMemoryProcessing);
            
            //Initialize parameters
            var totalLinesProcessed = 0L;
            var frontier = DateTime.MinValue;
            var updatedSymbols = new HashSet<Symbol>();
            var estimatedEndTime = _referenceDate.AddHours(16);

            // Create multithreaded readers; start them in threads and store the ticks in queues
            var readers = files.Select(file => new AlgoSeekOptionsReader(file, _referenceDate)).ToList();
            var parallelReaders = readers.Select(reader => new AlgoSeekParallelReader(reader, 10000)).ToList();
            var synchronizer = new SynchronizingEnumerator(parallelReaders);
>>>>>>> 4fdbe024fb90617e2e30fe079b04eca3fd72e4a7

            // Prime the synchronizer if required:
            if (synchronizer.Current == null)
            {
                synchronizer.MoveNext();
            }

            var start = DateTime.Now;
            Log.Trace("AlgoSeekOptionsConverter.Convert(): Synchronizing and processing ticks...", files.Count(), _referenceDate);
<<<<<<< HEAD

=======
>>>>>>> 4fdbe024fb90617e2e30fe079b04eca3fd72e4a7
            do
            {
                var tick = synchronizer.Current;
                frontier = tick.Time;
<<<<<<< HEAD
=======
                updatedSymbols.Add(tick.Symbol);
>>>>>>> 4fdbe024fb90617e2e30fe079b04eca3fd72e4a7

                //Add or create the consolidator-flush mechanism for symbol:
                List<AlgoSeekOptionsProcessor> symbolProcessors;
                if (!_processors.TryGetValue(tick.Symbol, out symbolProcessors))
                {
<<<<<<< HEAD
                    symbolProcessors = new List<AlgoSeekOptionsProcessor>(2)
                    {
                        new AlgoSeekOptionsProcessor(tick.Symbol, _referenceDate, TickType.Quote, resolution, _destination),
                        new AlgoSeekOptionsProcessor(tick.Symbol, _referenceDate, TickType.Trade, resolution, _destination)
                    };
=======
                    symbolProcessors = new List<AlgoSeekOptionsProcessor>(resolutions.Length);
                    foreach (var resolution in resolutions)
                    {
                        symbolProcessors.Add(new AlgoSeekOptionsProcessor(tick.Symbol, _referenceDate, TickType.Quote, resolution, _destination));
                        symbolProcessors.Add(new AlgoSeekOptionsProcessor(tick.Symbol, _referenceDate, TickType.Trade, resolution, _destination));
                    }
>>>>>>> 4fdbe024fb90617e2e30fe079b04eca3fd72e4a7
                    _processors[tick.Symbol] = symbolProcessors;
                }

                // Pass current tick into processor:
<<<<<<< HEAD
=======
                symbolProcessors = _processors[tick.Symbol];
>>>>>>> 4fdbe024fb90617e2e30fe079b04eca3fd72e4a7
                foreach (var unit in symbolProcessors)
                {
                    unit.Process(tick);
                }

                //Due to limits on the files that can be open at a time we need to constantly flush this to disk.
                totalLinesProcessed++;
                if (totalLinesProcessed % 1000000m == 0)
                {
                    var completed = Math.Round(1 - (estimatedEndTime - frontier).TotalMinutes / TimeSpan.FromHours(6.5).TotalMinutes, 3);
<<<<<<< HEAD
                    Log.Trace("AlgoSeekOptionsConverter.Convert(): Processed {0,3}M ticks( {1}k / sec ); Memory in use: {2} MB; Frontier Time: {3}; Completed: {4:P3}. ASOP Count: {5}", Math.Round(totalLinesProcessed / 1000000m, 2), Math.Round(totalLinesProcessed / 1000L / (DateTime.Now - start).TotalSeconds), Process.GetCurrentProcess().WorkingSet64 / (1024 * 1024), frontier.ToString("u"), completed, _processors.Count);
=======
                    Log.Trace("AlgoSeekOptionsConverter.Convert(): Processed {0,3}M ticks( {1}k / sec ); Memory in use: {2} MB; Frontier Time: {3}; Completed: {4:P3}", 
                        Math.Round(totalLinesProcessed / 1000000m, 2),
                        Math.Round(totalLinesProcessed/1000L / (DateTime.Now - start).TotalSeconds),
                        Process.GetCurrentProcess().WorkingSet64 / (1024 * 1024), 
                        frontier.ToString("u"), completed);
                }

                //Flush to disk occasionally for low ram/disk mode.
                if (!_inMemoryProcessing && totalLinesProcessed % _flushInterval == 0)
                {
                    Log.Trace("AlgoSeekOptionsConverter.Convert(): Writing memory buffer of {0} symbols to disk...", updatedSymbols.Count);
                    foreach (var updated in updatedSymbols)
                    {
                        _processors[updated].ForEach(x => x.FlushBuffer(frontier, _inMemoryProcessing, false));
                    }
                    updatedSymbols.Clear();
>>>>>>> 4fdbe024fb90617e2e30fe079b04eca3fd72e4a7
                }
            }
            while (synchronizer.MoveNext());

            Log.Trace("AlgoSeekOptionsConverter.Convert(): Performing final flush to disk... ");
            foreach (var symbol in _processors.Keys)
            {
<<<<<<< HEAD
                _processors[symbol].ForEach(x => x.FlushBuffer(DateTime.MaxValue, true));
=======
                _processors[symbol].ForEach(x => x.FlushBuffer(DateTime.MaxValue, _inMemoryProcessing, true));
>>>>>>> 4fdbe024fb90617e2e30fe079b04eca3fd72e4a7
            }

            Log.Trace("AlgoSeekOptionsConverter.Convert(): Finished processing directory: " + _source);
        }

        /// <summary>
<<<<<<< HEAD
        /// Compress the queue buffers directly to a zip file. Lightening fast as streaming ram-> compressed zip.
        /// </summary>
        public void Package(DateTime date)
        {
            var sources = _processors.Values;

            // Get the data by ticker values
            var quotes = sources.Select(x => x[0]).Where(x => x.Queue.Count > 0).GroupBy(process => process.Symbol.Value);
            var trades = sources.Select(x => x[1]).Where(x => x.Queue.Count > 0).GroupBy(process => process.Symbol.Value);
            var groups = new[] { quotes, trades };

            //Get total file count:
            var count = 0;
            var work = trades.Count() + quotes.Count();

            // Write each grouped zip:
            foreach (var tradeType in groups)
            foreach (var group in tradeType)
            {
                //All algoSeekProcessors share the same zippath.
                var zip = group.First().ZipPath;
                var files = new Dictionary<string, string>();
                foreach (var processor in group)
                {
                    // Get the file output for this processor:
                    files.Add(processor.EntryPath, FileBuilder(processor));
                }

                // Create the directory and the zip file
                var output = new DirectoryInfo(zip);
                if (!output.Parent.Exists) output.Parent.Create();
                Compression.ZipData(zip, files);
                if (count++ % 1000 == 0) Log.Trace("AlgoSeekOptionsConverter.Package(): Processed {0} of {1} files...", count, work);
=======
        /// Compress the queue buffers directly to a zip file
        /// </summary>
        public void MemoryCompress()
        {
            foreach (var processors in _processors.Values)
            {
                foreach (var processor in processors.Where(x => x.Queue.Count > 0))
                {
                    var output = new DirectoryInfo(processor.ZipPath);
                    if (!output.Parent.Exists) output.Parent.Create();
                    if (File.Exists(processor.ZipPath)) File.Delete(processor.ZipPath);
                    Compression.ZipData(processor.ZipPath, processor.EntryPath, processor.Queue);
                }
>>>>>>> 4fdbe024fb90617e2e30fe079b04eca3fd72e4a7
            }
        }

        /// <summary>
<<<<<<< HEAD
        /// Output a list of basedata objects into a string csv line.
        /// </summary>
        /// <param name="processor"></param>
        /// <returns></returns>
        private string FileBuilder(AlgoSeekOptionsProcessor processor)
        {
            var sb = new StringBuilder();
            foreach (var data in processor.Queue)
            {
                sb.AppendLine(LeanData.GenerateLine(data, SecurityType.Option, processor.Resolution));
            }
            return sb.ToString();
=======
        /// Point to a directory of option csv files and compress into a single option.zip for the day.
        /// </summary>
        /// <param name="dataDirectory">Directory with the option csv files</param>
        /// <param name="parallelism">Number of threads to use in the compression, defaults to 1</param>
        public void DirectoryCompress(string dataDirectory, int parallelism = 1)
        {
            Log.Trace("AlgoSeekOptionsConverter.Compress(): Begin compressing csv files");

            var root = Path.Combine(dataDirectory, "option", "usa");
            var fine =
                from res in _resolutions
                let path = Path.Combine(root, res.ToLower())
                from sym in Directory.EnumerateDirectories(path)
                from dir in Directory.EnumerateDirectories(sym)
                select new DirectoryInfo(dir).FullName;

            var options = new ParallelOptions {MaxDegreeOfParallelism = parallelism};
            Parallel.ForEach(fine, options, dir =>
            {
                try
                {
                    // zip the contents of the directory and then delete the directory
                    Compression.ZipDirectory(dir, dir + ".zip", false);
                    Directory.Delete(dir, true);
                    Log.Trace("Processed: " + dir);
                }
                catch (Exception err)
                {
                    Log.Error(err, "Zipping " + dir);
                }
            });
>>>>>>> 4fdbe024fb90617e2e30fe079b04eca3fd72e4a7
        }
    }
}
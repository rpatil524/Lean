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
using System.Text;
using QuantConnect.Data;
using QuantConnect.Data.Market;
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
        private DateTime _referenceDate;
        private Resolution[] _resolutions;
        private Dictionary<Symbol, List<AlgoSeekOptionsProcessor>> _processors;


        /// <summary>
        /// Create a new instance of the AlgoSeekOptions Converter. Parse a single input directory into an output.
        /// </summary>
        /// <param name="referenceDate">Datetime to be added to the milliseconds since midnight. Algoseek data is stored in channel files (XX.bz2) and in a source directory</param>
        /// <param name="source">Source directory of the .bz algoseek files</param>
        /// <param name="destination">Data directory of LEAN</param>
        public AlgoSeekOptionsConverter(DateTime referenceDate, string source, string destination)
        {
            _source = source;
            _referenceDate = referenceDate;
            _destination = destination;
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

            // Prime the synchronizer if required:
            if (synchronizer.Current == null)
            {
                synchronizer.MoveNext();
            }

            var start = DateTime.Now;
            Log.Trace("AlgoSeekOptionsConverter.Convert(): Synchronizing and processing ticks...", files.Count(), _referenceDate);

            do
            {
                var tick = synchronizer.Current;
                frontier = tick.Time;

                //Add or create the consolidator-flush mechanism for symbol:
                List<AlgoSeekOptionsProcessor> symbolProcessors;
                if (!_processors.TryGetValue(tick.Symbol, out symbolProcessors))
                {
                    symbolProcessors = new List<AlgoSeekOptionsProcessor>(2)
                    {
                        new AlgoSeekOptionsProcessor(tick.Symbol, _referenceDate, TickType.Quote, resolution, _destination),
                        new AlgoSeekOptionsProcessor(tick.Symbol, _referenceDate, TickType.Trade, resolution, _destination)
                    };
                    _processors[tick.Symbol] = symbolProcessors;
                }

                // Pass current tick into processor:
                foreach (var unit in symbolProcessors)
                {
                    unit.Process(tick);
                }

                //Due to limits on the files that can be open at a time we need to constantly flush this to disk.
                totalLinesProcessed++;
                if (totalLinesProcessed % 1000000m == 0)
                {
                    var completed = Math.Round(1 - (estimatedEndTime - frontier).TotalMinutes / TimeSpan.FromHours(6.5).TotalMinutes, 3);
                    Log.Trace("AlgoSeekOptionsConverter.Convert(): Processed {0,3}M ticks( {1}k / sec ); Memory in use: {2} MB; Frontier Time: {3}; Completed: {4:P3}. ASOP Count: {5}", Math.Round(totalLinesProcessed / 1000000m, 2), Math.Round(totalLinesProcessed / 1000L / (DateTime.Now - start).TotalSeconds), Process.GetCurrentProcess().WorkingSet64 / (1024 * 1024), frontier.ToString("u"), completed, _processors.Count);
                }
            }
            while (synchronizer.MoveNext());

            Log.Trace("AlgoSeekOptionsConverter.Convert(): Performing final flush to disk... ");
            foreach (var symbol in _processors.Keys)
            {
                _processors[symbol].ForEach(x => x.FlushBuffer(DateTime.MaxValue, true));
            }

            Log.Trace("AlgoSeekOptionsConverter.Convert(): Finished processing directory: " + _source);
        }

        /// <summary>
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
            }
        }

        /// <summary>
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
        }
    }
}
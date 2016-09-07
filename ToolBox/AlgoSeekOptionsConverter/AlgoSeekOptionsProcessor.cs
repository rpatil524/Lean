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
using QuantConnect.Data;
using System.Collections.Generic;
using System.IO;
using QuantConnect.Data.Consolidators;
using QuantConnect.Data.Market;
using QuantConnect.Util;
using System.Linq;

namespace QuantConnect.ToolBox.AlgoSeekOptionsConverter
{
    /// <summary>
    /// Processor for caching and consolidating ticks; 
    /// then flushing the ticks in memory to disk when triggered.
    /// </summary>
    public class AlgoSeekOptionsProcessor
    {
<<<<<<< HEAD
        private string _zipPath;
        private string _entryPath;
        private Symbol _symbol;
        private TickType _tickType;
        private Resolution _resolution;
        private Queue<BaseData> _queue;
=======
        private Symbol _symbol;
        private TickType _tickType;
        private Resolution _resolution;
        private Queue<string> _queue; 
>>>>>>> 4fdbe024fb90617e2e30fe079b04eca3fd72e4a7
        private string _dataDirectory;
        private IDataConsolidator _consolidator;
        private DateTime _referenceDate;
        private static string[] _windowsRestrictedNames =
        {
            "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5",
            "COM6", "COM7", "COM8", "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5",
            "LPT6", "LPT7", "LPT8", "LPT9"
        };

        /// <summary>
        /// Zip entry name for the option contract
        /// </summary>
        public string EntryPath
        {
<<<<<<< HEAD
            get
            {
                if (_entryPath == null)
                    _entryPath = LeanData.GenerateZipEntryName(_symbol, _referenceDate, _resolution, _tickType);
                return _entryPath;
            }
=======
            get { return LeanData.GenerateZipEntryName(_symbol, _referenceDate, _resolution, _tickType); }
>>>>>>> 4fdbe024fb90617e2e30fe079b04eca3fd72e4a7
        }

        /// <summary>
        /// Zip file path for the option contract collection
        /// </summary>
        public string ZipPath
        {
<<<<<<< HEAD
            get
            {
                if (_zipPath == null)
                    _zipPath = Path.Combine(_dataDirectory, LeanData.GenerateRelativeZipFilePath(Safe(_symbol), _referenceDate, _resolution, _tickType).Replace(".zip", string.Empty)) + ".zip";
                return _zipPath;
            }
        }

        /// <summary>
        /// Public access to the processor symbol
        /// </summary>
        public Symbol Symbol
        {
            get { return _symbol; }
=======
            get { return Path.Combine(_dataDirectory, LeanData.GenerateRelativeZipFilePath(Safe(_symbol), _referenceDate, _resolution, _tickType).Replace(".zip", string.Empty)) + ".zip"; }
>>>>>>> 4fdbe024fb90617e2e30fe079b04eca3fd72e4a7
        }

        /// <summary>
        /// Output base data queue for processing in memory
        /// </summary>
<<<<<<< HEAD
        public Queue<BaseData> Queue
=======
        public Queue<string> Queue
>>>>>>> 4fdbe024fb90617e2e30fe079b04eca3fd72e4a7
        {
            get { return _queue; }
        }

        /// <summary>
<<<<<<< HEAD
        /// Accessor for the final enumerator
        /// </summary>
        public Resolution Resolution
        {
            get { return _resolution; }
        }

        /// <summary>
=======
>>>>>>> 4fdbe024fb90617e2e30fe079b04eca3fd72e4a7
        /// Create a new AlgoSeekOptionsProcessor for enquing consolidated bars and flushing them to disk
        /// </summary>
        /// <param name="symbol">Symbol for the processor</param>
        /// <param name="date">Reference date for the processor</param>
        /// <param name="tickType">TradeBar or QuoteBar to generate</param>
        /// <param name="resolution">Resolution to consolidate</param>
        /// <param name="dataDirectory">Data directory for LEAN</param>
        public AlgoSeekOptionsProcessor(Symbol symbol, DateTime date, TickType tickType, Resolution resolution, string dataDirectory)
        {
            _symbol = Safe(symbol);
            _tickType = tickType;
<<<<<<< HEAD
            _referenceDate = date;
            _resolution = resolution;
            _queue = new Queue<BaseData>();
            _dataDirectory = dataDirectory;
            _consolidator = new TickConsolidator(resolution.ToTimeSpan());

            // Setup the consolidator for the requested resolution
            if (resolution == Resolution.Tick) throw new NotSupportedException();
            if (tickType == TickType.Quote)
            {
                _consolidator = new TickQuoteBarConsolidator(resolution.ToTimeSpan());
            }

            // On consolidating the bars put the bar into a queue in memory to be written to disk later.
            _consolidator.DataConsolidated += (sender, consolidated) =>
            {
                _queue.Enqueue(consolidated);
=======
            _resolution = resolution;
            _queue = new Queue<string>();
            _dataDirectory = dataDirectory;
            _referenceDate = date;
            
            // Setup the consolidator for the requested resolution
            _consolidator = new PassthroughConsolidator();
            if (resolution != Resolution.Tick)
            {
                if (tickType == TickType.Trade)
                {
                    _consolidator = new TickConsolidator(resolution.ToTimeSpan());
                }
                else
                {
                    _consolidator = new TickQuoteBarConsolidator(resolution.ToTimeSpan());
                }
            }
            
            // On consolidating the bars put the bar into a queue in memory to be written to disk later.
            _consolidator.DataConsolidated += (sender, consolidated) =>
            {
                _queue.Enqueue(ToCsv(consolidated));
>>>>>>> 4fdbe024fb90617e2e30fe079b04eca3fd72e4a7
            };
        }

        /// <summary>
        /// Process the tick; add to the con
        /// </summary>
        /// <param name="data"></param>
        public void Process(BaseData data)
        {
            if (((Tick)data).TickType != _tickType)
            {
                return;
            }

            _consolidator.Update(data);
        }

        /// <summary>
        /// Write the in memory queues to the disk.
        /// </summary>
        /// <param name="frontierTime">Current foremost tick time</param>
<<<<<<< HEAD
        /// <param name="finalFlush">Indicates is this is the final push to disk at the end of the data</param>
        public void FlushBuffer(DateTime frontierTime, bool finalFlush)
=======
        /// <param name="inMemoryProcessing">Process this option symbol entirely in memory</param>
        /// <param name="finalFlush">Indicates is this is the final push to disk at the end of the data</param>
        public void FlushBuffer(DateTime frontierTime, bool inMemoryProcessing, bool finalFlush)
>>>>>>> 4fdbe024fb90617e2e30fe079b04eca3fd72e4a7
        {
            //Force the consolidation if time has past the bar
            _consolidator.Scan(frontierTime);

            // If this is the final packet dump it to the queue
            if (finalFlush && _consolidator.WorkingData != null)
            {
<<<<<<< HEAD
                _queue.Enqueue(_consolidator.WorkingData);
=======
                _queue.Enqueue(ToCsv(_consolidator.WorkingData));  
            }

            // No need to write to disk
            if (inMemoryProcessing) return;

            // Purge the queue to disk if there's work to do:
            if (_queue.Count == 0) return;

            using (var writer = new LeanOptionsWriter(_dataDirectory, _symbol, frontierTime, _resolution, _tickType))
            {
                while (_queue.Count > 0)
                {
                    writer.WriteEntry(_queue.Dequeue());
                }
>>>>>>> 4fdbe024fb90617e2e30fe079b04eca3fd72e4a7
            }
        }

        /// <summary>
        /// Add filtering to safe check the symbol for windows environments
        /// </summary>
        /// <param name="symbol">Symbol to rename if required</param>
        /// <returns>Renamed symbol for reserved names</returns>
        private static Symbol Safe(Symbol symbol)
        {
            if (OS.IsWindows)
            {
                if (_windowsRestrictedNames.Contains(symbol.Value))
                {
                    symbol = Symbol.CreateOption("_" + symbol.Value, symbol.ID.Market, symbol.ID.OptionStyle,
                        symbol.ID.OptionRight, symbol.ID.StrikePrice, symbol.ID.Date);
                }
            }
            return symbol;
        }
<<<<<<< HEAD
=======

        /// <summary>
        /// Convert the basedata to a string.
        /// </summary>
        /// <param name="data"></param>
        /// <returns></returns>
        private string ToCsv(BaseData data)
        {
            return LeanData.GenerateLine(data, data.Symbol.ID.SecurityType, _resolution);
        }
    }

    /// <summary>
    /// This is a shim for handling Tick resolution data in TickRepository
    /// Ordinary TickConsolidators presents Consolidated data as type TradeBars.
    /// However, LeanData.GenerateLine expects Tick resolution data to be of type Tick.
    /// This class lets tick data pass through without changing object type,
    /// which simplifies the logic in TickRepository.
    /// </summary>
    internal class PassthroughConsolidator : IDataConsolidator
    {
        public BaseData Consolidated { get; private set; }

        public BaseData WorkingData
        {
            get { return null; }
        }

        public Type InputType
        {
            get { return typeof(BaseData); }
        }

        public Type OutputType
        {
            get { return typeof(BaseData); }
        }

        public event DataConsolidatedHandler DataConsolidated;

        public void Update(BaseData data)
        {
            Consolidated = data;
            if (DataConsolidated != null)
            {
                DataConsolidated(this, data);
            }
        }

        public void Scan(DateTime currentLocalTime)
        {
        }
>>>>>>> 4fdbe024fb90617e2e30fe079b04eca3fd72e4a7
    }
}
import { ScatterChart, Scatter, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend } from 'recharts';
import { Card, CardContent } from './ui/card';

function band2color(band: string) {
    switch (band) {
        case 'g':
            return '#38b000';
        case 'r':
            return '#ef233c';
        case 'i':
            return '#fcbf49';
        default:
            return '#FFFFFF';
    }
}

function jd2mjd(jd: number) {
    return jd - 2400000.5;
}

// Custom formatter to limit decimal places
const formatNumber = (value: number) => {
    return value.toFixed(2);
};

// Function to generate evenly spaced ticks
const generateTicks = (min: number, max: number, count: number) => {
    const ticks = [];
    const step = (max - min) / (count - 1);
    
    for (let i = 0; i < count; i++) {
        ticks.push(min + step * i);
    }
    
    return ticks;
};

function getArrayMinMax(arr: number[], field: string) {
    let min = arr[0][field];
    let max = arr[0][field];
    for (let i = 1; i < arr.length; i++) {
        if (arr[i][field] < min) {
            min = arr[i][field];
        }
        if (arr[i][field] > max) {
            max = arr[i][field];
        }
    }
    return { min, max };
}

export default function Lightcurve({
    data,
  }: {
    data: any
  }) {
    let detection_traces = {};
    let nondetection_traces = {};

    let { min: min_mag_det, max: max_mag_det } = getArrayMinMax(data.prv_candidates, 'magpsf');
    let { min: min_jd_det, max: max_jd_det } = getArrayMinMax(data.prv_candidates, 'jd');
    // do the same for the non detections
    let { min: min_mag_nondet, max: max_mag_nondet } = getArrayMinMax(data.prv_nondetections, 'diffmaglim');
    let { min: min_jd_nondet, max: max_jd_nondet } = getArrayMinMax(data.prv_nondetections, 'jd');
    // we want to take the min and max of both detections and non detections
    let min_mag = Math.min(min_mag_det, min_mag_nondet) - 0.5; // add some padding
    let max_mag = Math.max(max_mag_det, max_mag_nondet) + 0.5; // add some padding
    let min_jd = Math.min(min_jd_det, min_jd_nondet) - 1; // add some padding
    let max_jd = Math.max(max_jd_det, max_jd_nondet) + 1; // add some padding

    // Generate fixed number of ticks for each axis
    const xTicks = generateTicks(jd2mjd(min_jd), jd2mjd(max_jd), 6); // 6 ticks on X-axis
    const yTicks = generateTicks(min_mag, max_mag, 6); // 6 ticks on Y-axis
    
    // let's loop over the detections. for each the key is the filter name
    // the x is the jd and the y is the magpsf
    data.prv_candidates.forEach((detection: any) => {
        let band = detection.band;
        let key = `${band}`;
        let x = jd2mjd(detection.jd);
        let y = detection.magpsf;

        if (detection_traces[key] === undefined) {
            detection_traces[key] = [];
        }
        detection_traces[key].push({ x: x, y: y });
    });
    data.prv_nondetections.forEach((detection: any) => {
        let band = detection.band;
        let key = `${band}`;
        let x = jd2mjd(detection.jd);
        let y = detection.diffmaglim;
        if (nondetection_traces[key] === undefined) {
            nondetection_traces[key] = [];
        }
        nondetection_traces[key].push({ x: x, y: y });
    });

    // Custom tooltip formatter
    const CustomTooltip = ({ active, payload }: any) => {
        if (active && payload && payload.length) {
            return (
                <div className="bg-white p-2 border border-gray-300 rounded shadow">
                    <p className="font-medium">{`MJD: ${formatNumber(payload[0].value)}`}</p>
                    <p>{`Magnitude: ${formatNumber(payload[1].value)}`}</p>
                    <p>{`Filter: ${payload[0].name}`}</p>
                </div>
            );
        }
        return null;
    };

    return (
        <Card className="@container/card col-span-2 lg:col-span-2">
            <CardContent>
                <ResponsiveContainer width="100%" height={400}>
                    <ScatterChart
                    margin={{
                        top: 20,
                        right: 20,
                        bottom: 20,
                        left: 20,
                    }}
                    >
                    <CartesianGrid strokeDasharray="3 3" opacity={0.6} />
                    <XAxis 
                        type="number" 
                        dataKey="x" 
                        name="MJD" 
                        domain={[jd2mjd(min_jd), jd2mjd(max_jd)]} 
                        tickFormatter={formatNumber}
                        ticks={xTicks}
                        label={{ value: "Modified Julian Date", position: "bottom", offset: 0 }}
                    />
                    <YAxis 
                        type="number" 
                        dataKey="y" 
                        name="Magnitude" 
                        reversed={true} 
                        domain={[max_mag, min_mag]} 
                        tickFormatter={formatNumber}
                        ticks={yTicks}
                        label={{ value: "Magnitude", angle: -90, position: "left" }}
                    />
                    <Tooltip content={<CustomTooltip />} />
                    {/* Detections */}
                    {Object.entries(detection_traces).map(([key, value]) => {
                        return (
                            <Scatter 
                                key={key} 
                                name={`${key.toUpperCase()}-band`} 
                                data={value} 
                                fill={band2color(key)} 
                                shape="circle"
                                legendType="circle"
                                // Make points larger and visible
                                dot={{ r: 5, strokeWidth: 1, stroke: '#000000' }}
                            />
                        );
                    })}
                    {/* Non-detections */}
                    {Object.entries(nondetection_traces).map(([key, value]) => {
                        return (
                            <Scatter
                                key={`${key}-nondet`}
                                name={`${key.toUpperCase()}-band (non-detection)`}
                                data={value}
                                fill={band2color(key)}
                                shape="square"
                                legendType="square"
                                opacity={0.7}
                                // Make points larger and visible
                                dot={{ r: 5, strokeWidth: 1, stroke: '#000000' }}
                            />
                        );
                    })}
                    <Legend verticalAlign="top" align="right" />
                    </ScatterChart>
                </ResponsiveContainer>
            </CardContent>
        </Card>
    );
}

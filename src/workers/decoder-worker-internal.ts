// import {PointAttributes, PointAttribute, PointAttributeTypes} from "../../../loader/PointAttributes.js";

const typedArrayMapping: Record<string, any> = {
  'int8': Int8Array,
  'int16': Int16Array,
  'int32': Int32Array,
  'int64': Float64Array,
  'uint8': Uint8Array,
  'uint16': Uint16Array,
  'uint32': Uint32Array,
  'uint64': Float64Array,
  'float': Float32Array,
  'double': Float64Array,
};

class PointAttributeType {
  name: string;
  type: any;
  numElements: number;
  byteSize: number;
  description: string = '';
  range: [number, number] = [Infinity, -Infinity];

  constructor(name: string, type: any, numElements: number) {
    this.name = name;
    this.type = type;
    this.numElements = numElements;
    this.byteSize = this.numElements * this.type.size;
  }
}

const PointAttributeTypes = {
  DATA_TYPE_DOUBLE: { ordinal: 0, name: 'double', size: 8 },
  DATA_TYPE_FLOAT: { ordinal: 1, name: 'float', size: 4 },
  DATA_TYPE_INT8: { ordinal: 2, name: 'int8', size: 1 },
  DATA_TYPE_UINT8: { ordinal: 3, name: 'uint8', size: 1 },
  DATA_TYPE_INT16: { ordinal: 4, name: 'int16', size: 2 },
  DATA_TYPE_UINT16: { ordinal: 5, name: 'uint16', size: 2 },
  DATA_TYPE_INT32: { ordinal: 6, name: 'int32', size: 4 },
  DATA_TYPE_UINT32: { ordinal: 7, name: 'uint32', size: 4 },
  DATA_TYPE_INT64: { ordinal: 8, name: 'int64', size: 8 },
  DATA_TYPE_UINT64: { ordinal: 9, name: 'uint64', size: 8 },
};

const PointAttribute: Record<string, PointAttributeType> = {
  POSITION_CARTESIAN: new PointAttributeType('POSITION_CARTESIAN', PointAttributeTypes.DATA_TYPE_FLOAT, 3),
  RGBA_PACKED: new PointAttributeType('COLOR_PACKED', PointAttributeTypes.DATA_TYPE_INT8, 4),
  COLOR_PACKED: new PointAttributeType('COLOR_PACKED', PointAttributeTypes.DATA_TYPE_INT8, 4),
  RGB_PACKED: new PointAttributeType('COLOR_PACKED', PointAttributeTypes.DATA_TYPE_INT8, 3),
  NORMAL_FLOATS: new PointAttributeType('NORMAL_FLOATS', PointAttributeTypes.DATA_TYPE_FLOAT, 3),
  INTENSITY: new PointAttributeType('INTENSITY', PointAttributeTypes.DATA_TYPE_UINT16, 1),
  CLASSIFICATION: new PointAttributeType('CLASSIFICATION', PointAttributeTypes.DATA_TYPE_UINT8, 1),
  NORMAL_SPHEREMAPPED: new PointAttributeType('NORMAL_SPHEREMAPPED', PointAttributeTypes.DATA_TYPE_UINT8, 2),
  NORMAL_OCT16: new PointAttributeType('NORMAL_OCT16', PointAttributeTypes.DATA_TYPE_UINT8, 2),
  NORMAL: new PointAttributeType('NORMAL', PointAttributeTypes.DATA_TYPE_FLOAT, 3),
  RETURN_NUMBER: new PointAttributeType('RETURN_NUMBER', PointAttributeTypes.DATA_TYPE_UINT8, 1),
  NUMBER_OF_RETURNS: new PointAttributeType('NUMBER_OF_RETURNS', PointAttributeTypes.DATA_TYPE_UINT8, 1),
  SOURCE_ID: new PointAttributeType('SOURCE_ID', PointAttributeTypes.DATA_TYPE_UINT16, 1),
  INDICES: new PointAttributeType('INDICES', PointAttributeTypes.DATA_TYPE_UINT32, 1),
  SPACING: new PointAttributeType('SPACING', PointAttributeTypes.DATA_TYPE_FLOAT, 1),
  GPS_TIME: new PointAttributeType('GPS_TIME', PointAttributeTypes.DATA_TYPE_DOUBLE, 1),
};

const Potree: any = {};

export function handleMessage(event: MessageEvent) {
  let { buffer, pointAttributes, scale, name, min, max, size, offset, numPoints } = event.data;
  let view = new DataView(buffer);
  let attributeBuffers: any = {};
  let attributeOffset = 0;
  let bytesPerPoint = 0;

  for (let pointAttribute of pointAttributes.attributes) {
    bytesPerPoint += pointAttribute.byteSize;
  }

  let gridSize = 32;
  let grid = new Uint32Array(gridSize ** 3);
  let toIndex = (x: number, y: number, z: number) => {
    // min is already subtracted
    const dx = gridSize * x / size.x;
    const dy = gridSize * y / size.y;
    const dz = gridSize * z / size.z;

    const ix = Math.min(dx, gridSize - 1);
    const iy = Math.min(dy, gridSize - 1);
    const iz = Math.min(dz, gridSize - 1);

    const index = ix + iy * gridSize + iz * gridSize * gridSize;

    return index;
  };

  let numOccupiedCells = 0;
  for (let pointAttribute of pointAttributes.attributes) {

    if (['POSITION_CARTESIAN', 'position'].includes(pointAttribute.name)) {
      let buff = new ArrayBuffer(numPoints * 4 * 3);
      let positions = new Float32Array(buff);

      for (let j = 0; j < numPoints; j++) {
        const pointOffset = j * bytesPerPoint;

        const x = (view.getInt32(pointOffset + attributeOffset + 0, true) * scale[0]) + offset[0] - min.x;
        const y = (view.getInt32(pointOffset + attributeOffset + 4, true) * scale[1]) + offset[1] - min.y;
        const z = (view.getInt32(pointOffset + attributeOffset + 8, true) * scale[2]) + offset[2] - min.z;

        let index = toIndex(x, y, z);
        let count = grid[index]++;

        if (count === 0) {
          numOccupiedCells++;
        }

        positions[3 * j + 0] = x;
        positions[3 * j + 1] = y;
        positions[3 * j + 2] = z;
      }

      attributeBuffers[pointAttribute.name] = { buffer: buff, attribute: pointAttribute };
    } else if (['RGBA', 'rgba'].includes(pointAttribute.name)) {
      let buff = new ArrayBuffer(numPoints * 4);
      let colors = new Uint8Array(buff);

      for (let j = 0; j < numPoints; j++) {
        let pointOffset = j * bytesPerPoint;

        let r = view.getUint16(pointOffset + attributeOffset + 0, true);
        let g = view.getUint16(pointOffset + attributeOffset + 2, true);
        let b = view.getUint16(pointOffset + attributeOffset + 4, true);

        colors[4 * j + 0] = r > 255 ? r / 256 : r;
        colors[4 * j + 1] = g > 255 ? g / 256 : g;
        colors[4 * j + 2] = b > 255 ? b / 256 : b;
      }

      attributeBuffers[pointAttribute.name] = { buffer: buff, attribute: pointAttribute };
    } else {
      let buff = new ArrayBuffer(numPoints * 4);
      let f32 = new Float32Array(buff);

      let TypedArray = typedArrayMapping[pointAttribute.type.name];
      const preciseBuffer = new TypedArray(numPoints);

      let [offset, scale] = [0, 1];

      const getterMap: Record<string, any> = {
        'int8': view.getInt8,
        'int16': view.getInt16,
        'int32': view.getInt32,
        // "int64":  view.getInt64,
        'uint8': view.getUint8,
        'uint16': view.getUint16,
        'uint32': view.getUint32,
        // "uint64": view.getUint64,
        'float': view.getFloat32,
        'double': view.getFloat64,
      };
      const getter = getterMap[pointAttribute.type.name].bind(view);

      // compute offset and scale to pack larger types into 32 bit floats
      if (pointAttribute.type.size > 4) {
        let [amin, amax] = pointAttribute.range;
        offset = amin;
        scale = 1 / (amax - amin);
      }

      for (let j = 0; j < numPoints; j++) {
        let pointOffset = j * bytesPerPoint;
        let value = getter(pointOffset + attributeOffset, true);

        f32[j] = (value - offset) * scale;
        preciseBuffer[j] = value;
      }

      attributeBuffers[pointAttribute.name] = {
        buffer: buff,
        preciseBuffer: preciseBuffer,
        attribute: pointAttribute,
        offset: offset,
        scale: scale,
      };
    }

    attributeOffset += pointAttribute.byteSize;

  }

  let occupancy = numPoints / numOccupiedCells;
  // console.log(`${name}: #points: ${numPoints}: #occupiedCells: ${numOccupiedCells}, occupancy: ${occupancy} points/cell`);

  { // add indices
    let buff = new ArrayBuffer(numPoints * 4);
    let indices = new Uint32Array(buff);

    for (let i = 0; i < numPoints; i++) {
      indices[i] = i;
    }

    attributeBuffers['INDICES'] = { buffer: buff, attribute: PointAttribute.INDICES };
  }

  { // handle attribute vectors
    let vectors = pointAttributes.vectors;

    for (let vector of vectors) {

      let { name, attributes } = vector;
      let numVectorElements = attributes.length;
      let buffer = new ArrayBuffer(numVectorElements * numPoints * 4);
      let f32 = new Float32Array(buffer);

      let iElement = 0;
      for (let sourceName of attributes) {
        let sourceBuffer = attributeBuffers[sourceName];
        let { offset, scale } = sourceBuffer;
        let view = new DataView(sourceBuffer.buffer);

        const getter = view.getFloat32.bind(view);

        for (let j = 0; j < numPoints; j++) {
          let value = getter(j * 4, true);

          f32[j * numVectorElements + iElement] = (value / scale) + offset;
        }

        iElement++;
      }

      let vecAttribute = new PointAttributeType(name, PointAttributeTypes.DATA_TYPE_FLOAT, 3);

      attributeBuffers[name] = {
        buffer: buffer,
        attribute: vecAttribute,
      };

    }

  }

  // let duration = performance.now() - tStart;
  // let pointsPerMs = numPoints / duration;
  // console.log(`duration: ${duration.toFixed(1)}ms, #points: ${numPoints}, points/ms: ${pointsPerMs.toFixed(1)}`);

  let message = {
    buffer: buffer,
    attributeBuffers: attributeBuffers,
    density: occupancy,
  };

  let transferables = [];
  for (let property in message.attributeBuffers) {
    transferables.push(message.attributeBuffers[property].buffer);
  }
  transferables.push(buffer);

  postMessage(message, transferables as any);
};

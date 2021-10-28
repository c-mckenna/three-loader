import { Box3, BufferAttribute, BufferGeometry, Sphere, Vector3 } from 'three';
import {
  IPointAttribute,
  IVectorAttribute,
  POINT_ATTRIBUTE_TYPES,
  PointAttributeName,
  PointAttributes,
  PointAttributeType,
} from '../point-attributes';
import { IPointCloudTreeNode } from '../types';
import { createChildAABB } from '../utils/bounds';
import { getIndexFromName } from '../utils/utils';

interface Hierarchy {
  firstChunkSize: number;
  stepSize: number;
  depth: number;
}

interface BoundingBoxData {
  min: [number, number, number];
  max: [number, number, number];
}

export interface Attribute {
  name: string;
  description: string;
  size: number;
  numElements: number;
  elementSize: number;
  type: string;
  min: [number, number, number];
  max: [number, number, number];
}

export interface OctreeJSON {
  version: string;
  name: string;
  description: string;
  points: number;
  projection: string;
  hierarchy: Hierarchy;
  offset: [number, number, number];
  scale: [number, number, number];
  spacing: number;
  boundingBox: BoundingBoxData;
  encoding: 'DEFAULT' | 'BROTLI';
  attributes: Attribute[];
}

interface AttributeData {
  attribute: {
    name: PointAttributeName;
    type: PointAttributeType;
    byteSize: number;
    numElements: number;
  };
  buffer: ArrayBuffer;
}

interface WorkerResponse {
  data: {
    density: number;
    attributeBuffers: { [name: string]: AttributeData };
    indices: ArrayBuffer;
    tightBoundingBox: { min: number[]; max: number[] };
    mean: number[];
  };
}

class NodeLoader {
  private scale: [number, number, number];
  private offset: [number, number, number];
  private workers: Worker[] = [];
  private disposed: boolean = false;

  constructor(private url: string, private metadata: OctreeJSON, private attributes: PointAttributes) {
    this.scale = metadata.scale;
    this.offset = metadata.offset;
  }

  private getWorker(type: 'DEFAULT' | 'BROTLI'): Worker {
    const types = {
      DEFAULT: require('../workers/decoder.worker.js'),
      BROTLI: require('../workers/decoder.brotli.worker.js')
    };

    const worker = this.workers.pop();
    if (worker) {
      // console.log('Reusing a worker');
      return worker;
    }

    // console.log(`Creating new worker [type=${type}]`);
    const ctor = types[type];
    return new ctor();
  }

  private releaseWorker(worker: Worker): void {
    // console.log('Returning a worker');
    this.workers.push(worker);
  }

  async load(node: OctreeGeometryNode): Promise<void> {
    if (node.loading || node.loading) {
      return;
    }

    node.loading = true;
    // TODO: where does this happen in three-loader
    // this.octreeGeometry.numNodesLoading++;
    // this.octreeGeometry.needsUpdate = true;

    try {
      if (node.nodeType === 2) {
        await this.loadHierarchy(node);
      }

      const { byteOffset, byteSize } = node;

      const first = byteOffset;
      const last = byteOffset + byteSize - BigInt(1);

      let buffer;

      if (byteSize === BigInt(0)) {
        buffer = new ArrayBuffer(0);
        console.warn(`Loaded node with 0 bytes: ${node.name}`);
      } else {
        const response = await fetch(`${this.url}/../octree.bin`, {
          headers: {
            'Content-Type': 'multipart/byteranges',
            'Range': `bytes=${first}-${last}`,
          },
        });

        buffer = await response.arrayBuffer();
      }

      const worker = this.getWorker(this.metadata.encoding);

      worker.onmessage = (e: WorkerResponse) => {
        const data = e.data;
        const buffers = data.attributeBuffers;

        // Potree.workerPool.returnWorker(workerPath, worker);

        const geometry = new BufferGeometry();
        // geometry.boundingBox = this.boundingBox;

        for (const property in buffers) {
          const buffer = buffers[property].buffer;

          if (property === 'position') {
            geometry.setAttribute('position', new BufferAttribute(new Float32Array(buffer), 3));
          } else if (property === 'rgba') {
            geometry.setAttribute('color', new BufferAttribute(new Uint8Array(buffer), 4, true));
            // TODO: handle rgba the Potree way
            // geometry.setAttribute('color', new BufferAttribute(new Uint8Array(buffer), 4, true));
          } else if (property === 'NORMAL') {
            geometry.setAttribute('normal', new BufferAttribute(new Float32Array(buffer), 3));
          } else if (property === 'INDICES') {
            const bufferAttribute = new BufferAttribute(new Uint8Array(buffer), 4);
            bufferAttribute.normalized = true;
            geometry.setAttribute('indices', bufferAttribute);
          } else {
            const bufferAttribute = new BufferAttribute(new Float32Array(buffer), 1);

            // TODO: anys
            const batchAttribute = buffers[property].attribute;
            (bufferAttribute as any).potree = {
              offset: (buffers[property] as any).offset,
              scale: (buffers[property] as any).scale,
              preciseBuffer: (buffers[property] as any).preciseBuffer,
              range: (batchAttribute as any).range,
            };

            geometry.setAttribute(property, bufferAttribute);
          }
        }
        // indices ??

        node.density = data.density;
        node.geometry = geometry;
        node.loaded = true;
        node.loading = false;
        node.failed = false;

        // TODO: how to do this three-loader way
        /*this.octreeGeometry.numNodesLoading--;
        this.octreeGeometry.needsUpdate = true;*/

        this.releaseWorker(worker);

        // this.callbacks.forEach(callback => callback(node));
      };

      const pointAttributes = node.octreeGeometry.getPointAttributes();
      const scale = node.octreeGeometry.getScale();

      const offset = node.octreeGeometry.getOffset();
      const box = node.getBoundingBox();
      const min = offset.clone().add(box.min);
      const size = box.max.clone().sub(box.min);
      const max = min.clone().add(size);
      const numPoints = node.numPoints;

      const message = {
        name: node.name,
        buffer: buffer,
        pointAttributes: pointAttributes,
        scale: scale,
        min: min,
        max: max,
        size: size,
        offset: [offset.x, offset.y, offset.z],
        numPoints: numPoints
      };

      worker.postMessage(message, [message.buffer]);
    } catch (e) {
      node.loaded = false;
      node.loading = false;
      // TODO: three-loader style
      // Potree.numNodesLoading--;

      console.log(`Failed to load ${node.name}`);
      console.error(e);
    }
  }

  private async loadHierarchy(node: OctreeGeometryNode): Promise<void> {
    const { hierarchyByteOffset, hierarchyByteSize } = node;
    const hierarchyPath: string = `${this.url}/../hierarchy.bin`;
    const first: bigint = hierarchyByteOffset;
    const last: bigint = first + hierarchyByteSize - BigInt(1);

    const response = await fetch(hierarchyPath, {
      headers: {
        'Content-Type': 'multipart/byteranges',
        'Range': `bytes=${first}-${last}`,
      },
    });
    const buffer = await response.arrayBuffer();

    this.parseHierarchy(node, buffer);
  }

  private parseHierarchy(node: OctreeGeometryNode, buffer: ArrayBuffer): void {
    const view = new DataView(buffer);
    const bytesPerNode = 22;
    const numNodes = buffer.byteLength / bytesPerNode;

    const octree = node.octreeGeometry;
    const nodes: OctreeGeometryNode[] = new Array(numNodes);
    nodes[0] = node;
    let nodePos = 1;

    for (let i = 0; i < numNodes; i++) {
      const current = nodes[i];

      const type = view.getUint8(i * bytesPerNode + 0);
      const childMask = view.getUint8(i * bytesPerNode + 1);
      const numPoints = view.getUint32(i * bytesPerNode + 2, true);
      const byteOffset = view.getBigInt64(i * bytesPerNode + 6, true);
      const byteSize = view.getBigInt64(i * bytesPerNode + 14, true);

      if (current.nodeType === 2) {
        // replace proxy with real node
        current.byteOffset = byteOffset;
        current.byteSize = byteSize;
        current.numPoints = numPoints;
      } else if (type === 2) {
        // load proxy
        current.hierarchyByteOffset = byteOffset;
        current.hierarchyByteSize = byteSize;
        current.numPoints = numPoints;
      } else {
        // load real node
        current.byteOffset = byteOffset;
        current.byteSize = byteSize;
        current.numPoints = numPoints;
      }

      current.nodeType = type;

      if (current.nodeType === 2) {
        continue;
      }

      for (let childIndex = 0; childIndex < 8; childIndex++) {
        const childExists = ((1 << childIndex) & childMask) !== 0;

        if (!childExists) {
          continue;
        }

        const childName = current.name + childIndex;

        const childAABB = createChildAABB(current.getBoundingBox(), childIndex);
        const child = new OctreeGeometryNode(childName, octree, childAABB);
        child.name = childName;
        child.spacing = current.spacing / 2;
        child.level = current.level + 1;

        current.children[childIndex] = child;
        child.parent = current;

        nodes[nodePos] = child;
        nodePos++;
      }
    }
  }

  dispose(): void {
    this.workers.forEach(worker => worker.terminate());
    this.workers = [];

    this.disposed = true;
  }
}

export class OctreeLoader {
/*  version: Version;
  // boundingBox: Box3;
  scale: number;
  getUrl: GetUrlFn;
  disposed: boolean = false;
  xhrRequest: XhrRequest;
  callbacks: Callback[];

  // private workers: Worker[] = [];

  constructor({
                getUrl = (s: string) => Promise.resolve(s),
                metadata,
                xhrRequest,
              }: any) {
    if (typeof metadata.version === 'string') {
      this.version = new Version(metadata.version);
    } else {
      this.version = metadata.version;
    }

    this.xhrRequest = xhrRequest;
    this.getUrl = getUrl;
    // this.boundingBox = boundingBox;
    this.scale = metadata.scale;
    this.callbacks = [];
  }*/

  dispose(): void {
    console.log('dispose called on the loader');
  }

  static async load(url: string): Promise<OctreeGeometry> {
    const response = await fetch(url);
    const metadata: OctreeJSON = await response.json();

    const attributes = OctreeLoader.parseAttributes(metadata.attributes);

    const loader = new NodeLoader(url, metadata, attributes);
    const octree = new OctreeGeometry(url, metadata, attributes, loader);
    const root = new OctreeGeometryNode('r', octree, octree.getBoundingBox());
    root.nodeType = 2;
    root.hierarchyByteSize = BigInt(metadata.hierarchy.firstChunkSize);
    root.spacing = octree.getSpacing();

    octree.setRoot(root);
    loader.load(root);

    return octree;
  }

  static parseAttributes(jsonAttributes: Attribute[]): PointAttributes {
    // TODO: Refactor this so it's not all inline
    const PointAttributeTypes: Record<string, PointAttributeType> = {
      DATA_TYPE_DOUBLE: { ordinal: 0, name: 'double', size: 8 } as any,
      DATA_TYPE_FLOAT: { ordinal: 1, name: 'float', size: 4 } as any,
      DATA_TYPE_INT8: { ordinal: 2, name: 'int8', size: 1 } as any,
      DATA_TYPE_UINT8: { ordinal: 3, name: 'uint8', size: 1 } as any,
      DATA_TYPE_INT16: { ordinal: 4, name: 'int16', size: 2 } as any,
      DATA_TYPE_UINT16: { ordinal: 5, name: 'uint16', size: 2 } as any,
      DATA_TYPE_INT32: { ordinal: 6, name: 'int32', size: 4 } as any,
      DATA_TYPE_UINT32: { ordinal: 7, name: 'uint32', size: 4 } as any,
      DATA_TYPE_INT64: { ordinal: 8, name: 'int64', size: 8 } as any,
      DATA_TYPE_UINT64: { ordinal: 9, name: 'uint64', size: 8 } as any,
    };

    const typenameTypeattributeMap: Record<string, PointAttributeType> = {
      double: PointAttributeTypes.DATA_TYPE_DOUBLE,
      float: PointAttributeTypes.DATA_TYPE_FLOAT,
      int8: PointAttributeTypes.DATA_TYPE_INT8,
      uint8: PointAttributeTypes.DATA_TYPE_UINT8,
      int16: PointAttributeTypes.DATA_TYPE_INT16,
      uint16: PointAttributeTypes.DATA_TYPE_UINT16,
      int32: PointAttributeTypes.DATA_TYPE_INT32,
      uint32: PointAttributeTypes.DATA_TYPE_UINT32,
      int64: PointAttributeTypes.DATA_TYPE_INT64,
      uint64: PointAttributeTypes.DATA_TYPE_UINT64,
    };

    const attributes = new PointAttributes();

    const replacements: Record<string, string> = {
      rgb: 'rgba'
    };

    for (const jsonAttribute of jsonAttributes) {
      const { name, numElements, min, max } = jsonAttribute;
      const type: PointAttributeType = typenameTypeattributeMap[jsonAttribute.type];
      const potreeAttributeName: string = replacements[name] ? replacements[name] : name;

      const attribute: IPointAttribute =  {
        name: potreeAttributeName as any,
        type,
        numElements,
        byteSize: numElements * type.size,
        range: numElements === 1 ? [min[0], max[0]] : [min, max]
      };

      if (name === 'gps-time') { // HACK: Guard against bad gpsTime range in metadata, see potree/potree#909
        if (attribute.range && attribute.range[0] === attribute.range[1] && typeof attribute.range[1] === 'number') {
          attribute.range[1] += 1;
        }
      }

      attribute.initialRange = attribute.range;

      attributes.add(attribute as any);
    }

    // check if it has normals
    const hasNormals: boolean =
      attributes.attributes.find((a: any) => a.name === 'NormalX') !== undefined &&
      attributes.attributes.find((a: any) => a.name === 'NormalY') !== undefined &&
      attributes.attributes.find((a: any) => a.name === 'NormalZ') !== undefined;

    if (hasNormals) {
      const vector: IVectorAttribute = {
        name: 'NORMAL',
        attributes: ['NormalX', 'NormalY', 'NormalZ'],
      };
      attributes.addVector(vector);
    }

    return attributes;
  }
}

export class OctreeGeometry {
  private root: IPointCloudTreeNode | undefined;

  private spacing: number;
  private scale: [number, number, number];
  private projection: string;
  private offset: Vector3;
  private boundingBox: Box3;
  private tightBoundingBox: Box3;
  private boundingSphere: Sphere;
  private tightBoundingSphere: Sphere;
  private disposed: boolean = false;

  constructor(private url: string, private metadata: OctreeJSON, private pointAttributes: PointAttributes, private loader: NodeLoader) {
    this.spacing = metadata.spacing;
    this.scale = metadata.scale;
    this.projection = metadata.projection;

    const min = new Vector3(...metadata.boundingBox.min);
    const max = new Vector3(...metadata.boundingBox.max);
    const boundingBox = new Box3(min, max);

    const offset = min.clone();
    boundingBox.min.sub(offset);
    boundingBox.max.sub(offset);

    this.boundingBox = boundingBox;
    this.tightBoundingBox = boundingBox.clone();
    this.boundingSphere = boundingBox.getBoundingSphere(new Sphere());
    this.tightBoundingSphere = boundingBox.getBoundingSphere(new Sphere());
    this.offset = offset;
  }

  getLoader(): NodeLoader {
    return this.loader;
  }

  getBoundingBox(): Box3 {
    return this.boundingBox.clone();
  }

  getPointAttributes(): PointAttributes {
    return this.pointAttributes;
  }

  getScale(): [number, number, number] {
    return this.scale;
  }

  getOffset(): Vector3 {
    return this.offset;
  }

  getSpacing(): number {
    return this.spacing;
  }

  setRoot(root: OctreeGeometryNode): void {
    this.root = root;
  }

  dispose(): void {
    this.loader.dispose();
    this.root?.traverse(node => node.dispose());

    this.disposed = true;
  }
}

export class OctreeGeometryNode implements IPointCloudTreeNode {
  id: number = OctreeGeometryNode.idCount++;
  name: string;
  index: number;
  octreeGeometry: OctreeGeometry;
  loaded: boolean = false;
  loading: boolean = false;
  failed: boolean = false;
  boundingSphere: Sphere;
  hasChildren: boolean = false;
  readonly children: Array<IPointCloudTreeNode | null> = [
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
  ];
  numPoints: number = 0;
  geometry: BufferGeometry | undefined;
  density: number = 0;
  level: number = 0;
  parent: OctreeGeometryNode | null = null;
  oneTimeDisposeHandlers: (() => void)[] = [];
  spacing: number = 0;

  hierarchyByteOffset: bigint = BigInt(0);
  hierarchyByteSize: bigint = BigInt(0);
  byteSize: bigint = BigInt(0);
  byteOffset: bigint = BigInt(0);
  nodeType: number = 0;

  readonly isTreeNode: boolean = false;
  private static idCount = 0;

  constructor(name: string, octreeGeometry: OctreeGeometry, public boundingBox: Box3) {
    this.name = name;
    this.index = getIndexFromName(name);
    this.octreeGeometry = octreeGeometry;
    this.boundingSphere = boundingBox.getBoundingSphere(new Sphere());
  }

  isLeafNode: boolean = true; // TODO: what is this

  isGeometryNode(): boolean {
    return true;
  }

  getLevel(): number {
    return this.level;
  }

  isLoaded(): boolean {
    return this.loaded;
  }

  getBoundingSphere(): Sphere {
    return this.boundingSphere;
  }

  getBoundingBox(): Box3 {
    return this.boundingBox;
  }

  getChildren(): Array<IPointCloudTreeNode | null> {
    const children = [];

    for (let i = 0; i < 8; i++) {
      if (this.children[i]) {
        children.push(this.children[i]);
      }
    }

    return children;
  }

  load(): void {
    // TODO: three-loader style
    /*if (Potree.numNodesLoading >= Potree.maxNodesLoading) {
      return;
    }*/

    this.octreeGeometry.getLoader().load(this);
  }

  getNumPoints(): number {
    return this.numPoints;
  }

  traverse(cb: (node: IPointCloudTreeNode) => void, includeSelf?: boolean): void {
    const stack: IPointCloudTreeNode[] = includeSelf ? [this] : [];

    let current: IPointCloudTreeNode | undefined;

    while ((current = stack.pop()) !== undefined) {
      cb(current);

      for (const child of current.children) {
        if (child !== null) {
          stack.push(child);
        }
      }
    }
  }

  dispose(): void {
    if (!this.geometry || !this.parent) {
      return;
    }

    this.geometry.dispose();
    this.geometry = undefined;
    this.loaded = false;

    this.oneTimeDisposeHandlers.forEach(handler => handler());
    this.oneTimeDisposeHandlers = [];
  }
}

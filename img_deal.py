# -*- coding: utf-8 -*-

import os
import json
import random
import shutil


def convert_to_yolo(images_dir, annotations_dir, train_dir, test_dir, train_ratio=0.8):
    # 获取所有图像和标注文件路径
    image_files = [os.path.join(images_dir, f) for f in os.listdir(images_dir)]
    annotation_files = [os.path.join(annotations_dir, f) for f in os.listdir(annotations_dir)]

    # 建立类别名称到类别ID的映射
    class_to_id = {}

    # 将数据划分为训练集和测试集
    num_images = len(image_files)
    num_train = int(num_images * train_ratio)
    train_indices = random.sample(range(num_images), num_train)

    # 转换并保存训练集和测试集
    for i, image_file in enumerate(image_files):
        annotation_file = annotation_files[i]
        if i in train_indices:
            train_image_dir = os.path.join(train_dir, 'images')
            train_label_dir = os.path.join(train_dir, 'labels')
            os.makedirs(train_image_dir, exist_ok=True)
            os.makedirs(train_label_dir, exist_ok=True)
            shutil.copy(image_file, train_image_dir)
            convert_annotation(annotation_file, train_label_dir, i, class_to_id)
        else:
            test_image_dir = os.path.join(test_dir, 'images')
            test_label_dir = os.path.join(test_dir, 'labels')
            os.makedirs(test_image_dir, exist_ok=True)
            os.makedirs(test_label_dir, exist_ok=True)
            shutil.copy(image_file, test_image_dir)
            convert_annotation(annotation_file, test_label_dir, i, class_to_id)


def convert_annotation(annotation_file, label_dir, index, class_to_id):
    with open(annotation_file, 'r') as f:
        data = json.load(f)

    image_width = data['imageWidth']
    image_height = data['imageHeight']

    for shape in data['shapes']:
        if shape['shape_type'] == 'rectangle':
            label = shape['label']
            if label not in class_to_id:
                class_to_id[label] = len(class_to_id)
            class_id = class_to_id[label]
            x1, y1 = shape['points'][0]
            x2, y2 = shape['points'][1]
            x = (x1 + x2) / (2 * image_width)
            y = (y1 + y2) / (2 * image_height)
            w = (x2 - x1) / image_width
            h = (y2 - y1) / image_height
            label_file = os.path.join(label_dir, f"{index}.txt")
            with open(label_file, 'w') as f:
                f.write(f"{class_id} {x} {y} {w} {h}")

# 调用函数进行转换
# convert_to_yolo('your_project/images', 'your_project/annotations', 'your_project/train', 'your_project/test')


if __name__ == '__main__':

    annotation_file = "./img/d.json"

    with open(annotation_file, 'r') as f:
        data = json.load(f)
    image_width = data['imageWidth']
    image_height = data['imageHeight']
    for shape in data['shapes']:
        if shape['shape_type'] == 'rectangle':
            x1, y1 = shape['points'][0]
            x2, y2 = shape['points'][1]
            x = (x1 + x2) / (2 * image_width)
            y = (y1 + y2) / (2 * image_height)
            w = (x2 - x1) / image_width
            h = (y2 - y1) / image_height
            label_file = os.path.join("./img", f"d.txt")
            with open(label_file, 'w') as f:
                f.write(f"0 {x} {y} {w} {h}")